use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::ws::Message;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::mpsc::UnboundedSender;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct WsClientHandle {
    pub id: u64,
    pub sender: UnboundedSender<Message>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobState {
    #[serde(default)]
    pub started: bool,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub stage: String,
    #[serde(default)]
    pub bytes: u64,
    #[serde(default)]
    pub total: Option<u64>,
    #[serde(default)]
    pub percent: Option<u64>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub last_activity: f64,
    #[serde(skip, default)]
    pub clients: Vec<WsClientHandle>,
}

impl Default for JobState {
    fn default() -> Self {
        Self::new()
    }
}

impl JobState {
    pub fn new() -> Self {
        Self {
            started: false,
            status: "pending".to_string(),
            stage: "pending".to_string(),
            bytes: 0,
            total: None,
            percent: None,
            error: None,
            last_activity: unix_time_seconds(),
            clients: Vec::new(),
        }
    }

    pub fn reset(
        &mut self,
        started: bool,
        status: impl Into<String>,
        stage: impl Into<String>,
        total: Option<u64>,
    ) {
        self.started = started;
        self.status = status.into();
        self.stage = stage.into();
        self.bytes = 0;
        self.total = total;
        self.percent = None;
        self.error = None;
        self.last_activity = unix_time_seconds();
    }

    pub fn snapshot(&self) -> Self {
        self.clone()
    }
}

pub fn unix_time_seconds() -> f64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_secs_f64()
}

pub fn new_job_state() -> JobState {
    JobState::new()
}

pub fn reset_job_state(
    state: &mut JobState,
    started: bool,
    status: impl Into<String>,
    stage: impl Into<String>,
    total: Option<u64>,
) {
    state.reset(started, status, stage, total);
}

pub fn state_event_payload(
    event: &str,
    state: &JobState,
    extra: Option<Map<String, Value>>,
) -> Value {
    let mut payload = Map::new();
    payload.insert("event".to_string(), Value::String(event.to_string()));
    payload.insert("bytes".to_string(), Value::from(state.bytes));
    payload.insert(
        "total".to_string(),
        state.total.map(Value::from).unwrap_or(Value::Null),
    );
    payload.insert("status".to_string(), Value::String(state.status.clone()));
    payload.insert("stage".to_string(), Value::String(state.stage.clone()));
    if state.percent.is_some() || event == "progress" {
        payload.insert(
            "percent".to_string(),
            state.percent.map(Value::from).unwrap_or(Value::Null),
        );
    }
    if let Some(extra) = extra {
        payload.extend(extra);
    }
    Value::Object(payload)
}

pub fn next_client_id() -> u64 {
    NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
}
