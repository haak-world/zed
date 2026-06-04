use anyhow::Context as _;
use futures::channel::mpsc;
use futures::StreamExt;
use gpui::{
    actions, div, list, px, Action, App, AsyncWindowContext, Context, Entity, EventEmitter,
    FocusHandle, Focusable, IntoElement, KeyDownEvent, ListAlignment, ListState, ParentElement,
    Pixels, Render, SharedString, Styled, Task, WeakEntity, Window,
};
use markdown::{Markdown, MarkdownElement, MarkdownFont, MarkdownStyle};
use serde_json::{json, Value};
use tungstenite::{connect, Message};
use ui::prelude::*;
use workspace::{
    Workspace,
    dock::{DockPosition, Panel, PanelEvent},
};

actions!(haak_chat, [Toggle, ToggleFocus]);

const HAAK_CHAT_KEY: &str = "HaakChat";

// --- Data model ---

#[derive(Clone)]
enum ChatEntry {
    User { text: SharedString },
    Assistant { markdown: Entity<Markdown>, done: bool },
    ToolCall { name: SharedString, input_summary: SharedString, result: Option<SharedString>, is_error: bool },
    System { text: SharedString },
}

// --- Panel ---

pub struct HaakChat {
    width: Option<Pixels>,
    focus_handle: FocusHandle,
    _workspace: WeakEntity<Workspace>,
    entries: Vec<ChatEntry>,
    list_state: ListState,
    input_text: String,
    session_id: Option<String>,
    session_name: Option<String>,
    agent: String,
    model: String,
    state: String,
    ws_write_tx: std::sync::mpsc::Sender<String>,
    _ws_task: Task<()>,
    _event_task: Task<()>,
}

impl HaakChat {
    pub async fn load(
        workspace: WeakEntity<Workspace>,
        mut cx: AsyncWindowContext,
    ) -> anyhow::Result<Entity<Self>> {
        workspace
            .update_in(&mut cx, |workspace, window, cx| Self::new(workspace, window, cx))
            .context("loading haak chat panel")
    }

    fn new(
        _workspace: &mut Workspace,
        _window: &mut Window,
        cx: &mut Context<Workspace>,
    ) -> Entity<Self> {
        let workspace_handle = cx.entity().downgrade();
        let (event_tx, event_rx) = mpsc::unbounded::<String>();
        // Write channel: std::sync because consumed in a blocking thread
        let (write_tx, write_rx) = std::sync::mpsc::channel::<String>();

        // Background OS thread: sync WebSocket I/O
        std::thread::spawn(move || {
            let url = "ws://127.0.0.1:5201/ws/session";
            let (mut ws, _) = match connect(url) {
                Ok(pair) => pair,
                Err(e) => {
                    let _ = event_tx.unbounded_send(
                        json!({"type":"error","message":format!("Connect failed: {e}")}).to_string()
                    );
                    return;
                }
            };

            let create = json!({"type":"session.create","agent":"bala","model":""});
            let _ = ws.send(Message::text(create.to_string()));

            // Non-blocking read so we can interleave with writes
            if let tungstenite::stream::MaybeTlsStream::Plain(s) = ws.get_ref() {
                let _ = s.set_nonblocking(true);
            }

            loop {
                // Read server events
                match ws.read() {
                    Ok(msg) => {
                        if msg.is_text() {
                            let text = msg.to_text().map(|s| s.to_string()).unwrap_or_default();
                            if event_tx.unbounded_send(text).is_err() { break; }
                        } else if msg.is_close() {
                            break;
                        }
                    }
                    Err(tungstenite::Error::Io(ref e))
                        if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => break,
                }

                // Send any queued user messages
                while let Ok(payload) = write_rx.try_recv() {
                    let _ = ws.send(Message::text(payload));
                }

                std::thread::sleep(std::time::Duration::from_millis(16));
            }
        });

        let ws_task = Task::ready(());

        cx.new(|cx| {
            let mut event_rx = event_rx;

            let event_task = cx.spawn(async |this: WeakEntity<HaakChat>, cx| {
                #[allow(unused_mut)]
                let mut event_rx = event_rx;
                while let Some(raw) = event_rx.next().await {
                    let _ = this.update(cx, |this, cx| {
                        this.handle_event(&raw, cx);
                    });
                }
            });

            let list_state = ListState::new(0, ListAlignment::Bottom, px(300.));

            Self {
                width: None,
                focus_handle: cx.focus_handle(),
                _workspace: workspace_handle,
                entries: Vec::new(),
                list_state,
                input_text: String::new(),
                session_id: None,
                session_name: None,
                agent: "bala".into(),
                model: String::new(),
                state: "connecting".into(),
                ws_write_tx: write_tx,
                _ws_task: ws_task,
                _event_task: event_task,
            }
        })
    }

    fn s(msg: &Value, key: &str) -> String {
        msg.get(key).and_then(|v| v.as_str()).unwrap_or("").to_string()
    }

    fn handle_event(&mut self, raw: &str, cx: &mut Context<Self>) {
        let Ok(msg) = serde_json::from_str::<Value>(raw) else { return };
        let t = Self::s(&msg, "type");

        match t.as_str() {
            "session.created" => {
                self.session_id = Some(Self::s(&msg, "sessionId"));
                self.session_name = Some(Self::s(&msg, "name"));
                self.agent = Self::s(&msg, "agent");
                if self.agent.is_empty() { self.agent = "bala".into(); }
                self.model = Self::s(&msg, "model");
                self.state = "connected".into();
                let label = format!("Connected as {}", self.agent);
                self.entries.push(ChatEntry::System { text: SharedString::from(label) });
                self.rebuild_list(cx);
            }
            "session.state" | "state" => {
                let s = msg.get("state").or(msg.get("value")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                if !s.is_empty() { self.state = s; }
                cx.notify();
            }
            "text" => {
                let delta = Self::s(&msg, "delta");
                if delta.is_empty() { return; }
                match self.entries.last_mut() {
                    Some(ChatEntry::Assistant { markdown, done: false, .. }) => {
                        markdown.update(cx, |md, cx| md.append(&delta, cx));
                    }
                    _ => {
                        let md = cx.new(|cx| Markdown::new_text(SharedString::from(delta), cx));
                        self.entries.push(ChatEntry::Assistant { markdown: md, done: false });
                        self.rebuild_list(cx);
                    }
                }
                cx.notify();
            }
            "tool_start" => {
                let name = Self::s(&msg, "name");
                self.entries.push(ChatEntry::ToolCall {
                    name: SharedString::from(name),
                    input_summary: SharedString::default(),
                    result: None, is_error: false,
                });
                self.rebuild_list(cx);
            }
            "tool_input" => {
                let summary = Self::s(&msg, "summary");
                if let Some(ChatEntry::ToolCall { input_summary, .. }) = self.entries.last_mut() {
                    *input_summary = SharedString::from(summary);
                    cx.notify();
                }
            }
            "tool_result" => {
                let content: String = Self::s(&msg, "content").chars().take(200).collect();
                let err = msg.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false);
                for entry in self.entries.iter_mut().rev() {
                    if let ChatEntry::ToolCall { result, is_error, .. } = entry {
                        *result = Some(SharedString::from(content));
                        *is_error = err;
                        break;
                    }
                }
                cx.notify();
            }
            "turn_done" => {
                for entry in self.entries.iter_mut().rev() {
                    if let ChatEntry::Assistant { done, .. } = entry { *done = true; break; }
                }
                self.state = "idle".into();
                cx.notify();
            }
            "error" => {
                let m = Self::s(&msg, "message");
                self.entries.push(ChatEntry::System { text: SharedString::from(format!("Error: {m}")) });
                self.rebuild_list(cx);
            }
            "session.ended" => {
                self.state = "ended".into();
                self.entries.push(ChatEntry::System { text: "Session ended".into() });
                self.rebuild_list(cx);
            }
            _ => {}
        }
    }

    fn rebuild_list(&mut self, cx: &mut Context<Self>) {
        self.list_state.reset(self.entries.len());
        cx.notify();
    }

    fn send_message(&mut self, cx: &mut Context<Self>) {
        let text = self.input_text.trim().to_string();
        if text.is_empty() { return; }
        self.entries.push(ChatEntry::User { text: SharedString::from(text.clone()) });
        self.input_text.clear();
        self.state = "running".into();
        self.rebuild_list(cx);

        let sid = self.session_id.clone().unwrap_or_default();
        let payload = json!({"type":"user.message","sessionId":sid,"text":text}).to_string();
        let _ = self.ws_write_tx.send(payload);
    }

    fn render_entry(&self, ix: usize, window: &Window, cx: &App) -> gpui::AnyElement {
        let entry = &self.entries[ix];
        match entry {
            ChatEntry::User { text } => div()
                .id(ElementId::NamedInteger("msg".into(), ix as u64))
                .px_4().py_2()
                .child(div().text_xs().text_color(cx.theme().colors().text_muted).child("you"))
                .child(div().text_sm().text_color(cx.theme().colors().text).child(text.clone()))
                .into_any_element(),

            ChatEntry::Assistant { markdown, .. } => {
                let style = MarkdownStyle::themed(MarkdownFont::Agent, window, cx);
                div()
                    .id(ElementId::NamedInteger("msg".into(), ix as u64))
                    .px_4().py_2()
                    .child(div().text_xs().text_color(cx.theme().colors().terminal_ansi_cyan)
                        .child(SharedString::from(self.agent.clone())))
                    .child(div().text_sm().child(MarkdownElement::new(markdown.clone(), style)))
                    .into_any_element()
            }

            ChatEntry::ToolCall { name, input_summary, result, is_error } => {
                let color = if result.is_some() {
                    if *is_error { cx.theme().colors().terminal_ansi_red }
                    else { cx.theme().colors().terminal_ansi_green }
                } else { cx.theme().colors().text_disabled };
                let dot: &str = if result.is_some() { "●" } else { "○" };
                div()
                    .id(ElementId::NamedInteger("msg".into(), ix as u64))
                    .px_4().py_1()
                    .child(div().flex().gap_2()
                        .child(div().text_xs().text_color(color).child(dot))
                        .child(div().text_xs().text_color(cx.theme().colors().text_muted).child(name.clone()))
                        .child(div().text_xs().text_color(cx.theme().colors().text_disabled).child(input_summary.clone())))
                    .into_any_element()
            }

            ChatEntry::System { text } => div()
                .id(ElementId::NamedInteger("msg".into(), ix as u64))
                .px_4().py_1()
                .child(div().text_xs().text_color(cx.theme().colors().text_disabled).italic().child(text.clone()))
                .into_any_element(),
        }
    }
}

// --- Trait impls ---

impl Panel for HaakChat {
    fn persistent_name() -> &'static str { "Haak Chat" }
    fn panel_key() -> &'static str { HAAK_CHAT_KEY }
    fn position(&self, _: &Window, _: &App) -> DockPosition { DockPosition::Right }
    fn position_is_valid(&self, p: DockPosition) -> bool { matches!(p, DockPosition::Left | DockPosition::Right) }
    fn set_position(&mut self, _: DockPosition, _: &mut Window, _: &mut Context<Self>) {}
    fn default_size(&self, _: &Window, _: &App) -> Pixels { px(420.0) }
    fn icon(&self, _: &Window, _: &App) -> Option<IconName> { Some(IconName::Chat) }
    fn icon_tooltip(&self, _: &Window, _: &App) -> Option<&'static str> { Some("HaaK Chat") }
    fn toggle_action(&self) -> Box<dyn Action> { Box::new(ToggleFocus) }
    fn activation_priority(&self) -> u32 { 11 }
}

impl Focusable for HaakChat {
    fn focus_handle(&self, _: &App) -> FocusHandle { self.focus_handle.clone() }
}

impl EventEmitter<PanelEvent> for HaakChat {}

impl Render for HaakChat {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state_color = match self.state.as_str() {
            "running" => cx.theme().colors().terminal_ansi_green,
            "idle" | "connected" => cx.theme().colors().terminal_ansi_cyan,
            "connecting" => cx.theme().colors().terminal_ansi_yellow,
            _ => cx.theme().colors().terminal_ansi_red,
        };

        let header = div()
            .px_3().py_2().border_b_1().border_color(cx.theme().colors().border)
            .flex().items_center().gap_2()
            .child(div().w(px(6.)).h(px(6.)).rounded_full().bg(state_color))
            .child(div().text_sm().text_color(cx.theme().colors().text)
                .font_weight(gpui::FontWeight::MEDIUM)
                .child(SharedString::from(self.session_name.clone().unwrap_or_else(|| self.agent.clone()))))
            .child(div().text_xs().text_color(cx.theme().colors().text_disabled)
                .child(SharedString::from(self.state.clone())));

        let weak = cx.entity().downgrade();
        let message_list = list(self.list_state.clone(), move |ix, window, cx| {
            weak.upgrade()
                .map(|e| e.read(cx).render_entry(ix, window, cx))
                .unwrap_or_else(|| div().into_any_element())
        })
        .flex_1()
        .size_full();

        let input_display = if self.input_text.is_empty() {
            "Write a message...".to_string()
        } else {
            self.input_text.clone()
        };
        let input_color = if self.input_text.is_empty() {
            cx.theme().colors().text_disabled
        } else {
            cx.theme().colors().text
        };

        let input = div()
            .px_3().py_2().border_t_1().border_color(cx.theme().colors().border)
            .child(div().px_2().py_1().rounded_md()
                .bg(cx.theme().colors().editor_background)
                .text_sm().text_color(input_color)
                .child(SharedString::from(input_display)));

        div()
            .key_context("HaakChat")
            .track_focus(&self.focus_handle)
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, _window, cx| {
                let ks = &event.keystroke;
                if ks.key == "enter" && !ks.modifiers.modified() {
                    this.send_message(cx);
                } else if ks.key == "backspace" {
                    this.input_text.pop();
                    cx.notify();
                } else if let Some(ch) = &ks.key_char {
                    if !ch.is_empty() && ch.chars().all(|c| !c.is_control()) {
                        this.input_text.push_str(ch);
                        cx.notify();
                    }
                }
            }))
            .size_full()
            .bg(cx.theme().colors().panel_background)
            .flex().flex_col()
            .child(header)
            .child(message_list)
            .child(input)
    }
}
