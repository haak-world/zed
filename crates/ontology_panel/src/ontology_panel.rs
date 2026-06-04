use anyhow::Context as _;
// Serialization deferred — db::kvp API changed in upstream
use gpui::{
    Action, App, AsyncWindowContext, Context, Entity, EventEmitter, FocusHandle, Focusable,
    InteractiveElement, IntoElement, ListSizingBehavior, ParentElement, Pixels, Render,
    SharedString, Styled, Task, UniformListScrollHandle, WeakEntity, Window, actions, div, px,
    uniform_list,
};
use serde::{Deserialize, Serialize};
use sqlez::connection::Connection;
use std::ops::Range;
use std::path::PathBuf;
use ui::prelude::*;
use util::ResultExt;
use workspace::{
    Workspace,
    dock::{DockPosition, Panel, PanelEvent},
};

actions!(ontology_panel, [Toggle, ToggleFocus,]);

const ONTOLOGY_PANEL_KEY: &str = "OntologyPanel";

#[derive(Debug, Clone)]
struct OntologyEntity {
    id: String,
    name: String,
    entity_type: String,
}

#[derive(Debug, Clone)]
struct BelongingRow {
    entity_id: String,
    entity_name: String,
    entity_type: String,
    quality: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
enum ViewMode {
    TypeList,
    Search,
}

pub struct OntologyPanel {
    width: Option<Pixels>,
    focus_handle: FocusHandle,
    _workspace: WeakEntity<Workspace>,
    scroll_handle: UniformListScrollHandle,
    detail_scroll_handle: UniformListScrollHandle,
    db_path: Option<PathBuf>,
    // Main list
    entities: Vec<OntologyEntity>,
    selected_idx: Option<usize>,
    // Detail: two sections
    belongs_to: Vec<BelongingRow>,
    contains: Vec<BelongingRow>,
    // Navigation history
    history: Vec<String>,
    history_pos: Option<usize>,
    // Search
    search_query: String,
    view_mode: ViewMode,
    // Stats
    total_entities: usize,
    total_belongings: usize,
    pending_serialization: Task<Option<()>>,
}

#[derive(Serialize, Deserialize)]
struct SerializedOntologyPanel {
    width: Option<f32>,
}

fn open_db(path: &PathBuf) -> Option<Connection> {
    Connection::open_file(path.to_str()?).into()
}

fn query_stats(conn: &Connection) -> (usize, usize) {
    let entities = conn
        .select::<(String, String)>("SELECT CAST(COUNT(*) AS TEXT), '' FROM entities")
        .ok()
        .and_then(|mut s| s().ok())
        .and_then(|v| v.into_iter().next())
        .and_then(|(c, _)| c.parse::<usize>().ok())
        .unwrap_or(0);
    let belongings = conn
        .select::<(String, String)>("SELECT CAST(COUNT(*) AS TEXT), '' FROM relations")
        .ok()
        .and_then(|mut s| s().ok())
        .and_then(|v| v.into_iter().next())
        .and_then(|(c, _)| c.parse::<usize>().ok())
        .unwrap_or(0);
    (entities, belongings)
}

fn query_type_summary(conn: &Connection) -> Vec<OntologyEntity> {
    let Ok(mut select) = conn.select::<(String, String)>(
        "SELECT type_ref, CAST(COUNT(*) AS TEXT) as c FROM entities GROUP BY type_ref ORDER BY COUNT(*) DESC",
    ) else {
        return Vec::new();
    };
    select()
        .unwrap_or_default()
        .into_iter()
        .map(|(entity_type, count)| OntologyEntity {
            id: format!("__type__:{}", entity_type),
            name: format!("{} ({})", entity_type, count),
            entity_type,
        })
        .collect()
}

fn query_entities_by_type(conn: &Connection, entity_type: &str) -> Vec<OntologyEntity> {
    let Ok(mut select) = conn.select_bound::<&str, (String, String, String)>(
        "SELECT id, COALESCE(name, id), type_ref FROM entities WHERE type_ref = ? ORDER BY name LIMIT 500",
    ) else {
        return Vec::new();
    };
    select(entity_type)
        .unwrap_or_default()
        .into_iter()
        .map(|(id, name, entity_type)| OntologyEntity {
            id,
            name,
            entity_type,
        })
        .collect()
}

fn query_search(conn: &Connection, query: &str) -> Vec<OntologyEntity> {
    let pattern = format!("%{}%", query);
    let Ok(mut select) = conn.select_bound::<&str, (String, String, String)>(
        "SELECT id, COALESCE(name, id), type_ref FROM entities WHERE name LIKE ? ORDER BY type_ref, name LIMIT 200",
    ) else {
        return Vec::new();
    };
    select(&pattern)
        .unwrap_or_default()
        .into_iter()
        .map(|(id, name, entity_type)| OntologyEntity {
            id,
            name,
            entity_type,
        })
        .collect()
}

fn query_belongs_to(conn: &Connection, entity_id: &str) -> Vec<BelongingRow> {
    let Ok(mut select) = conn.select_bound::<&str, (String, String, Option<String>)>(
        "SELECT r.target_id, COALESCE(e.type_ref, '?'), r.quality_ref
         FROM relations r LEFT JOIN entities e ON e.id = r.target_id
         WHERE r.entity_id = ? ORDER BY r.quality_ref, r.target_id LIMIT 200",
    ) else {
        return Vec::new();
    };
    select(entity_id)
        .unwrap_or_default()
        .into_iter()
        .map(|(target, target_type, quality)| {
            let name = target.split(':').last().unwrap_or(&target).to_string();
            BelongingRow {
                entity_id: target.clone(),
                entity_name: name,
                entity_type: target_type,
                quality,
            }
        })
        .collect()
}

fn query_contains(conn: &Connection, entity_id: &str) -> Vec<BelongingRow> {
    let Ok(mut select) = conn.select_bound::<&str, (String, String, String, Option<String>)>(
        "SELECT e.id, COALESCE(e.name, e.id), e.type_ref, r.quality_ref
         FROM relations r JOIN entities e ON e.id = r.entity_id
         WHERE r.target_id = ? ORDER BY e.type_ref, e.name LIMIT 200",
    ) else {
        return Vec::new();
    };
    select(entity_id)
        .unwrap_or_default()
        .into_iter()
        .map(|(id, name, entity_type, quality)| BelongingRow {
            entity_id: id,
            entity_name: name,
            entity_type,
            quality,
        })
        .collect()
}

impl OntologyPanel {
    pub async fn load(
        workspace: WeakEntity<Workspace>,
        mut cx: AsyncWindowContext,
    ) -> anyhow::Result<Entity<Self>> {
        workspace
            .update_in(&mut cx, |workspace, window, cx| {
                Self::new(workspace, window, cx)
            })
            .context("loading ontology panel")
    }

    fn new(
        workspace: &mut Workspace,
        _window: &mut Window,
        cx: &mut Context<Workspace>,
    ) -> Entity<Self> {
        let workspace_handle = cx.entity().downgrade();

        let db_path = workspace.worktrees(cx).next().and_then(|wt| {
            let root = wt.read(cx).abs_path();
            for rel in &["infra/var/haak.db", "data/entities.db"] {
                let candidate = root.join(rel);
                if candidate.exists() {
                    return Some(candidate.to_path_buf());
                }
            }
            None
        }).or_else(|| {
            // Fallback: check well-known absolute path
            let fallback = PathBuf::from("/Users/zach/Projects/haak/infra/var/haak.db");
            fallback.exists().then_some(fallback)
        });

        let (entities, total_entities, total_belongings) = db_path
            .as_ref()
            .and_then(open_db)
            .map(|conn| {
                let (te, tb) = query_stats(&conn);
                let ents = query_type_summary(&conn);
                (ents, te, tb)
            })
            .unwrap_or_default();

        cx.new(|cx| Self {
            width: None,
            focus_handle: cx.focus_handle(),
            _workspace: workspace_handle,
            scroll_handle: UniformListScrollHandle::new(),
            detail_scroll_handle: UniformListScrollHandle::new(),
            db_path,
            entities,
            selected_idx: None,
            belongs_to: Vec::new(),
            contains: Vec::new(),
            history: Vec::new(),
            history_pos: None,
            search_query: String::new(),
            view_mode: ViewMode::TypeList,
            total_entities,
            total_belongings,
            pending_serialization: Task::ready(None),
        })
    }

    fn navigate_to(&mut self, entity_id: &str, cx: &mut Context<Self>) {
        let Some(conn) = self.db_path.as_ref().and_then(open_db) else {
            return;
        };

        // Check if it's a type summary row
        if entity_id.starts_with("__type__:") {
            let entity_type = &entity_id["__type__:".len()..];
            self.entities = query_entities_by_type(&conn, entity_type);
            self.selected_idx = None;
            self.belongs_to.clear();
            self.contains.clear();
            self.view_mode = ViewMode::TypeList;
            cx.notify();
            return;
        }

        // Push to history
        if let Some(pos) = self.history_pos {
            self.history.truncate(pos + 1);
        }
        self.history.push(entity_id.to_string());
        self.history_pos = Some(self.history.len() - 1);

        // Load detail
        self.belongs_to = query_belongs_to(&conn, entity_id);
        self.contains = query_contains(&conn, entity_id);

        // Find and select in current list
        self.selected_idx = self
            .entities
            .iter()
            .position(|e| e.id == entity_id);

        cx.notify();
    }

    fn go_back(&mut self, cx: &mut Context<Self>) {
        if let Some(pos) = self.history_pos {
            if pos > 0 {
                self.history_pos = Some(pos - 1);
                let id = self.history[pos - 1].clone();
                let Some(conn) = self.db_path.as_ref().and_then(open_db) else {
                    return;
                };
                self.belongs_to = query_belongs_to(&conn, &id);
                self.contains = query_contains(&conn, &id);
                self.selected_idx = self.entities.iter().position(|e| e.id == id);
                cx.notify();
            }
        }
    }

    fn show_type_list(&mut self, cx: &mut Context<Self>) {
        let Some(conn) = self.db_path.as_ref().and_then(open_db) else {
            return;
        };
        self.entities = query_type_summary(&conn);
        self.selected_idx = None;
        self.belongs_to.clear();
        self.contains.clear();
        self.search_query.clear();
        self.view_mode = ViewMode::TypeList;
        cx.notify();
    }

    fn do_search(&mut self, cx: &mut Context<Self>) {
        if self.search_query.len() < 2 {
            return;
        }
        let Some(conn) = self.db_path.as_ref().and_then(open_db) else {
            return;
        };
        self.entities = query_search(&conn, &self.search_query);
        self.selected_idx = None;
        self.belongs_to.clear();
        self.contains.clear();
        self.view_mode = ViewMode::Search;
        cx.notify();
    }

    fn select_entity(&mut self, ix: usize, cx: &mut Context<Self>) {
        let entity_id = self.entities[ix].id.clone();
        self.navigate_to(&entity_id, cx);
    }

    fn select_belonging(&mut self, entity_id: &str, cx: &mut Context<Self>) {
        // Navigate into a belonging row — load it as primary
        let id = entity_id.to_string();
        self.navigate_to(&id, cx);
    }

    // Serialization deferred — upstream db::kvp API changed

    fn type_color(&self, entity_type: &str, cx: &Context<Self>) -> gpui::Hsla {
        match entity_type {
            "project" => cx.theme().colors().terminal_ansi_blue,
            "persona" => cx.theme().colors().terminal_ansi_green,
            "foundation" => cx.theme().colors().terminal_ansi_yellow,
            "pattern" => cx.theme().colors().terminal_ansi_magenta,
            "person" => cx.theme().colors().terminal_ansi_cyan,
            "paper" => cx.theme().colors().terminal_ansi_red,
            "tag" => cx.theme().colors().terminal_ansi_bright_green,
            "track" => cx.theme().colors().terminal_ansi_bright_magenta,
            "file" => cx.theme().colors().text_muted,
            _ => cx.theme().colors().text_disabled,
        }
    }

    fn render_entity_row(
        &self,
        ix: usize,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let entity = &self.entities[ix];
        let selected = self.selected_idx == Some(ix);
        let type_color = self.type_color(&entity.entity_type, cx);

        let bg = if selected {
            cx.theme().colors().ghost_element_selected
        } else {
            gpui::Hsla::transparent_black()
        };

        div()
            .id(ElementId::NamedInteger("entity".into(), ix as u64))
            .px_2()
            .py_0p5()
            .bg(bg)
            .hover(|s| s.bg(cx.theme().colors().ghost_element_hover))
            .on_click(cx.listener(move |this, _, _, cx| this.select_entity(ix, cx)))
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(
                        div()
                            .text_color(type_color)
                            .text_xs()
                            .min_w(px(64.))
                            .child(SharedString::from(entity.entity_type.clone())),
                    )
                    .child(
                        div()
                            .text_color(cx.theme().colors().text)
                            .text_sm()
                            .child(SharedString::from(entity.name.clone())),
                    ),
            )
    }

    fn render_belonging_row(
        &self,
        row: &BelongingRow,
        section: &'static str,
        ix: usize,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let type_color = self.type_color(&row.entity_type, cx);
        let quality_text = row.quality.as_deref().unwrap_or("");
        let entity_id = row.entity_id.clone();

        div()
            .id(ElementId::NamedInteger(section.into(), ix as u64))
            .px_3()
            .py_0p5()
            .cursor_pointer()
            .hover(|s| s.bg(cx.theme().colors().ghost_element_hover))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.select_belonging(&entity_id, cx);
            }))
            .child(
                div()
                    .flex()
                    .gap_2()
                    .children(if !quality_text.is_empty() {
                        Some(
                            div()
                                .text_color(cx.theme().colors().text_muted)
                                .text_xs()
                                .child(SharedString::from(quality_text.to_string())),
                        )
                    } else {
                        None
                    })
                    .child(
                        div()
                            .text_color(type_color)
                            .text_xs()
                            .child(SharedString::from(row.entity_type.clone())),
                    )
                    .child(
                        div()
                            .text_color(cx.theme().colors().text)
                            .text_sm()
                            .child(SharedString::from(row.entity_name.clone())),
                    ),
            )
    }

    fn render_section_header(
        &self,
        label: &str,
        count: usize,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        div()
            .px_2()
            .py_1()
            .border_t_1()
            .border_color(cx.theme().colors().border)
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().colors().text_muted)
                    .child(SharedString::from(format!("{} ({})", label, count))),
            )
    }
}

impl Panel for OntologyPanel {
    fn persistent_name() -> &'static str {
        "Ontology Panel"
    }

    fn panel_key() -> &'static str {
        ONTOLOGY_PANEL_KEY
    }

    fn position(&self, _: &Window, _: &App) -> DockPosition {
        DockPosition::Right
    }

    fn position_is_valid(&self, position: DockPosition) -> bool {
        matches!(position, DockPosition::Left | DockPosition::Right)
    }

    fn set_position(&mut self, _: DockPosition, _: &mut Window, _: &mut Context<Self>) {}

    fn default_size(&self, _: &Window, _: &App) -> Pixels {
        px(360.0)
    }

    fn icon(&self, _: &Window, _: &App) -> Option<IconName> {
        Some(IconName::DatabaseZap)
    }

    fn icon_tooltip(&self, _: &Window, _: &App) -> Option<&'static str> {
        Some("Ontology Panel")
    }

    fn toggle_action(&self) -> Box<dyn Action> {
        Box::new(ToggleFocus)
    }

    fn activation_priority(&self) -> u32 {
        12
    }
}

impl Focusable for OntologyPanel {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for OntologyPanel {}

impl Render for OntologyPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let entity_count = self.entities.len();
        let has_db = self.db_path.is_some();
        let has_selection = self.selected_idx.is_some();
        let belongs_to_count = self.belongs_to.len();
        let contains_count = self.contains.len();

        // Header with stats
        let header = div()
            .px_2()
            .py_1()
            .border_b_1()
            .border_color(cx.theme().colors().border)
            .flex()
            .justify_between()
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().colors().text_muted)
                    .child(SharedString::from(format!(
                        "{} entities · {} edges",
                        self.total_entities, self.total_belongings
                    ))),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().colors().text_disabled)
                    .id("home-btn")
                    .cursor_pointer()
                    .hover(|s| s.text_color(gpui::Hsla { h: 0.0, s: 0.0, l: 0.7, a: 1.0 }))
                    .on_click(cx.listener(|this, _, _, cx| this.show_type_list(cx)))
                    .child("home"),
            );

        // Selected entity name
        let selected_header = if let Some(idx) = self.selected_idx {
            let entity = &self.entities[idx];
            let type_color = self.type_color(&entity.entity_type, cx);
            Some(
                div()
                    .px_2()
                    .py_1()
                    .border_b_1()
                    .border_color(cx.theme().colors().border)
                    .bg(cx.theme().colors().ghost_element_selected)
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(
                                div()
                                    .text_color(type_color)
                                    .text_xs()
                                    .child(SharedString::from(entity.entity_type.clone())),
                            )
                            .child(
                                div()
                                    .text_color(cx.theme().colors().text)
                                    .text_sm()
                                    .font_weight(gpui::FontWeight::SEMIBOLD)
                                    .child(SharedString::from(entity.name.clone())),
                            ),
                    ),
            )
        } else {
            None
        };

        // Main entity list
        let entity_list = uniform_list(
            "ontology-entities",
            entity_count,
            cx.processor(|this: &mut Self, range: Range<usize>, window, cx| {
                range
                    .map(|ix| this.render_entity_row(ix, window, cx).into_any_element())
                    .collect()
            }),
        )
        .with_sizing_behavior(ListSizingBehavior::Infer)
        .track_scroll(&self.scroll_handle);

        // Detail sections
        let detail = if has_selection {
            let mut detail_div = div().flex().flex_col();

            if belongs_to_count > 0 {
                let belongs_to_snapshot: Vec<_> = self.belongs_to.clone();
                detail_div = detail_div
                    .child(self.render_section_header("belongs to", belongs_to_count, cx))
                    .child(uniform_list(
                        "belongs-to",
                        belongs_to_count,
                        cx.processor(move |this: &mut Self, range: Range<usize>, window, cx| {
                            range
                                .map(|ix| {
                                    this.render_belonging_row(
                                        &belongs_to_snapshot[ix],
                                        "bt",
                                        ix,
                                        window,
                                        cx,
                                    )
                                    .into_any_element()
                                })
                                .collect()
                        }),
                    )
                    .with_sizing_behavior(ListSizingBehavior::Infer));
            }

            if contains_count > 0 {
                let contains_snapshot: Vec<_> = self.contains.clone();
                detail_div = detail_div
                    .child(self.render_section_header("contains", contains_count, cx))
                    .child(uniform_list(
                        "contains",
                        contains_count,
                        cx.processor(move |this: &mut Self, range: Range<usize>, window, cx| {
                            range
                                .map(|ix| {
                                    this.render_belonging_row(
                                        &contains_snapshot[ix],
                                        "ct",
                                        ix,
                                        window,
                                        cx,
                                    )
                                    .into_any_element()
                                })
                                .collect()
                        }),
                    )
                    .with_sizing_behavior(ListSizingBehavior::Infer));
            }

            if belongs_to_count == 0 && contains_count == 0 {
                detail_div = detail_div.child(
                    div()
                        .px_2()
                        .py_1()
                        .border_t_1()
                        .border_color(cx.theme().colors().border)
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().colors().text_disabled)
                                .child("no connections"),
                        ),
                );
            }

            Some(detail_div)
        } else {
            None
        };

        let body = if has_db {
            div()
                .flex()
                .flex_col()
                .size_full()
                .child(div().flex_1().child(entity_list))
                .children(detail)
        } else {
            div().p_4().child(
                div()
                    .text_sm()
                    .text_color(cx.theme().colors().text_muted)
                    .child("No entities.db found in worktree"),
            )
        };

        div()
            .key_context("OntologyPanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(cx.theme().colors().panel_background)
            .child(header)
            .children(selected_header)
            .child(body)
    }
}
