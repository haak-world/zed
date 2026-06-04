mod connection;

use agent_servers::{AgentServer, AgentServerDelegate};
use acp_thread::AgentConnection;
use anyhow::Result;
use gpui::{App, Entity, Task};
use project::{AgentId, Project};
use std::rc::Rc;
use std::sync::Arc;
use ui::IconName;

use crate::connection::HaakConnection;

pub struct HaakAgentServer {
    url: Arc<str>,
}

impl HaakAgentServer {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.into(),
        }
    }
}

impl AgentServer for HaakAgentServer {
    fn agent_id(&self) -> AgentId {
        AgentId::new("haak")
    }

    fn logo(&self) -> IconName {
        IconName::DatabaseZap
    }

    fn connect(
        &self,
        _delegate: AgentServerDelegate,
        _project: Entity<Project>,
        _cx: &mut App,
    ) -> Task<Result<Rc<dyn AgentConnection>>> {
        let url = self.url.clone();
        Task::ready(Ok(Rc::new(HaakConnection::new(&url)) as Rc<dyn AgentConnection>))
    }

    fn into_any(self: Rc<Self>) -> Rc<dyn std::any::Any> {
        self
    }
}
