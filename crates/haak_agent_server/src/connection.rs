use acp_thread::{AcpThread, AgentConnection, UserMessageId};
use agent_client_protocol::schema as acp;
use anyhow::Result;
use gpui::{App, AppContext as _, Entity, SharedString, Task, WeakEntity};
use project::{AgentId, Project};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use tungstenite::{connect, Message};
use util::path_list::PathList;

pub struct HaakConnection {
    url: Arc<str>,
    sessions: Rc<RefCell<HashMap<acp::SessionId, HaakSession>>>,
}

struct HaakSession {
    #[allow(dead_code)]
    thread: WeakEntity<AcpThread>,
    haakd_session_id: String,
    write_tx: std::sync::mpsc::Sender<String>,
    event_rx: Arc<StdMutex<Option<futures::channel::mpsc::UnboundedReceiver<String>>>>,
}

use std::sync::Mutex as StdMutex;

impl HaakConnection {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.into(),
            sessions: Rc::new(RefCell::new(HashMap::new())),
        }
    }
}

impl AgentConnection for HaakConnection {
    fn agent_id(&self) -> AgentId {
        AgentId::new("haak")
    }

    fn telemetry_id(&self) -> SharedString {
        "haak".into()
    }

    fn auth_methods(&self) -> &[acp::AuthMethod] {
        &[]
    }

    fn authenticate(&self, _method: acp::AuthMethodId, _cx: &mut App) -> Task<Result<()>> {
        Task::ready(Ok(()))
    }

    fn new_session(
        self: Rc<Self>,
        project: Entity<Project>,
        work_dirs: PathList,
        cx: &mut App,
    ) -> Task<Result<Entity<AcpThread>>> {
        let url = self.url.clone();
        let sessions = self.sessions.clone();
        let this = self.clone();

        cx.spawn(async move |cx| {
            let (event_tx, event_rx) = futures::channel::mpsc::unbounded::<String>();
            let (write_tx, write_rx) = std::sync::mpsc::channel::<String>();

            let ws_url = format!("{}/ws/session", url.replace("http", "ws"));

            std::thread::spawn(move || {
                let (mut ws, _) = match connect(&*ws_url) {
                    Ok(pair) => pair,
                    Err(e) => {
                        let _ = event_tx.unbounded_send(
                            json!({"type":"error","message":format!("{e}")}).to_string()
                        );
                        return;
                    }
                };

                let create = json!({"type":"session.create","agent":"bala","model":""});
                let _ = ws.send(Message::text(create.to_string()));

                if let tungstenite::stream::MaybeTlsStream::Plain(s) = ws.get_ref() {
                    let _ = s.set_nonblocking(true);
                }

                loop {
                    match ws.read() {
                        Ok(msg) if msg.is_text() => {
                            let text = msg.to_text().map(|s| s.to_string()).unwrap_or_default();
                            if event_tx.unbounded_send(text).is_err() { break; }
                        }
                        Ok(msg) if msg.is_close() => break,
                        Err(tungstenite::Error::Io(ref e))
                            if e.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(_) => break,
                        _ => {}
                    }

                    while let Ok(payload) = write_rx.try_recv() {
                        let _ = ws.send(Message::text(payload));
                    }

                    std::thread::sleep(std::time::Duration::from_millis(16));
                }
            });

            use futures::StreamExt;
            let mut event_rx = event_rx;
            let mut haakd_sid = String::new();

            while let Some(raw) = event_rx.next().await {
                if let Ok(msg) = serde_json::from_str::<Value>(&raw) {
                    let t = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if t == "session.created" {
                        haakd_sid = msg.get("sessionId")
                            .and_then(|v| v.as_str()).unwrap_or("").to_string();
                        break;
                    }
                    if t == "error" {
                        anyhow::bail!("haakd: {}", msg.get("message").and_then(|v| v.as_str()).unwrap_or("?"));
                    }
                }
            }

            if haakd_sid.is_empty() {
                anyhow::bail!("haakd session not created");
            }

            let session_id = acp::SessionId::new(haakd_sid.clone());

            let thread = cx.update(|cx| {
                let action_log = cx.new(|_| action_log::ActionLog::new(project.clone()));
                cx.new(|cx| {
                    AcpThread::new(
                        None,
                        None,
                        Some(work_dirs),
                        this.clone(),
                        project,
                        action_log,
                        session_id.clone(),
                        watch::Receiver::constant(acp::PromptCapabilities::new()),
                        cx,
                    )
                })
            });

            sessions.borrow_mut().insert(session_id.clone(), HaakSession {
                thread: thread.downgrade(),
                haakd_session_id: haakd_sid,
                write_tx,
                event_rx: Arc::new(StdMutex::new(Some(event_rx))),
            });

            Ok(thread)
        })
    }

    fn prompt(
        &self,
        _id: UserMessageId,
        params: acp::PromptRequest,
        cx: &mut App,
    ) -> Task<Result<acp::PromptResponse>> {
        let sessions = self.sessions.borrow();
        let Some(session) = sessions.get(&params.session_id) else {
            return Task::ready(Err(anyhow::anyhow!("session not found")));
        };

        // Extract text from content blocks
        let text: String = params.prompt.iter()
            .filter_map(|block| match block {
                acp::ContentBlock::Text(t) => Some(t.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Send user message to haakd
        let payload = json!({
            "type": "user.message",
            "sessionId": session.haakd_session_id,
            "text": text,
        }).to_string();
        let _ = session.write_tx.send(payload);

        // Take the event receiver to read in this task
        let event_rx_holder = session.event_rx.clone();
        let thread = session.thread.clone();

        cx.spawn(async move |mut cx| {
            let mut event_rx = event_rx_holder.lock().unwrap().take()
                .ok_or_else(|| anyhow::anyhow!("event_rx already taken"))?;

            use futures::StreamExt;
            while let Some(raw) = event_rx.next().await {
                let Ok(msg) = serde_json::from_str::<Value>(&raw) else { continue };
                let t = msg.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string();

                let update: Option<acp::SessionUpdate> = match t.as_str() {
                    "text" => {
                        let delta = msg.get("delta").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        if delta.is_empty() { continue; }
                        Some(acp::SessionUpdate::AgentMessageChunk(
                            acp::ContentChunk::new(acp::ContentBlock::from(delta))
                        ))
                    }
                    "thinking" => {
                        let txt = msg.get("text").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        if txt.is_empty() { continue; }
                        Some(acp::SessionUpdate::AgentThoughtChunk(
                            acp::ContentChunk::new(acp::ContentBlock::from(txt))
                        ))
                    }
                    "tool_start" => {
                        let name = msg.get("name").and_then(|v| v.as_str()).unwrap_or("tool").to_string();
                        let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("unknown").to_string();
                        Some(acp::SessionUpdate::ToolCall(acp::ToolCall::new(id, name)))
                    }
                    "tool_result" => {
                        let id = msg.get("tool_use_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let is_error = msg.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false);
                        let status = if is_error { acp::ToolCallStatus::Failed } else { acp::ToolCallStatus::Completed };
                        let mut fields = acp::ToolCallUpdateFields::new();
                        fields.status = Some(status);
                        Some(acp::SessionUpdate::ToolCallUpdate(acp::ToolCallUpdate::new(id, fields)))
                    }
                    "turn_done" => {
                        *event_rx_holder.lock().unwrap() = Some(event_rx);
                        return Ok(acp::PromptResponse::new(acp::StopReason::EndTurn));
                    }
                    "error" => {
                        *event_rx_holder.lock().unwrap() = Some(event_rx);
                        let err_msg = msg.get("message").and_then(|v| v.as_str()).unwrap_or("unknown error").to_string();
                        return Err(anyhow::anyhow!(err_msg));
                    }
                    "session.ended" => {
                        return Ok(acp::PromptResponse::new(acp::StopReason::EndTurn));
                    }
                    _ => None,
                };

                if let Some(update) = update {
                    let _ = cx.update(|cx| {
                        if let Some(t) = thread.upgrade() {
                            t.update(cx, |thread, cx| {
                                let _ = thread.handle_session_update(update, cx);
                            });
                        }
                    });
                }
            }

            Ok(acp::PromptResponse::new(acp::StopReason::EndTurn))
        })
    }

    fn cancel(&self, _session_id: &acp::SessionId, _cx: &mut App) {}

    fn into_any(self: Rc<Self>) -> Rc<dyn std::any::Any> {
        self
    }
}

