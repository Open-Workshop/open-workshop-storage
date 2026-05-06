use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Request, Uri};
use bytes::Bytes;
use futures_util::FutureExt;
use http_body_util::Full;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use lru::LruCache;
use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::auth;
use crate::config::AppConfig;
use crate::fs_utils::safe_path;
use crate::limits::ConcurrencyLimiter;
use crate::redis_store::{RedisBackend, RedisEventCallback};
use crate::state::{
    new_job_state, state_event_payload, unix_time_seconds, JobState, WsClientHandle,
};

struct ListenerState {
    users: usize,
    task: Option<JoinHandle<()>>,
}

pub struct AppState {
    pub config: Arc<AppConfig>,
    pub main_dir: PathBuf,
    pub manager_url: String,
    pub temp_dir: PathBuf,
    pub job_state: Arc<Mutex<HashMap<String, JobState>>>,
    pub job_meta: Arc<Mutex<HashMap<String, Value>>>,
    blurhash_cache: Option<Arc<Mutex<LruCache<String, (String, u32, u32)>>>>,
    redis_backend: Option<Arc<RedisBackend>>,
    listener_state: Arc<Mutex<ListenerState>>,
    pub upload_limiter: ConcurrencyLimiter,
    pub download_limiter: ConcurrencyLimiter,
    pub repack_limiter: ConcurrencyLimiter,
    pub progress_push_interval: Duration,
}

impl AppState {
    pub fn new(config: AppConfig) -> anyhow::Result<Arc<Self>> {
        let main_dir = config.main_dir.clone();
        let temp_dir = main_dir.join("temp");
        std::fs::create_dir_all(&main_dir)?;
        std::fs::create_dir_all(main_dir.join("archive"))?;
        std::fs::create_dir_all(main_dir.join("resource"))?;
        std::fs::create_dir_all(main_dir.join("avatar"))?;
        std::fs::create_dir_all(&temp_dir)?;

        let blurhash_cache = if config.blurhash_cache_size == 0 {
            None
        } else {
            let capacity = NonZeroUsize::new(config.blurhash_cache_size)
                .unwrap_or_else(|| NonZeroUsize::new(1).unwrap());
            Some(Arc::new(Mutex::new(LruCache::new(capacity))))
        };

        let redis_backend = match config.redis_url.as_deref() {
            Some(redis_url) if !redis_url.trim().is_empty() => Some(Arc::new(RedisBackend::new(
                redis_url,
                config.redis_prefix.clone(),
            )?)),
            _ => None,
        };

        Ok(Arc::new(Self {
            main_dir,
            manager_url: config.manager_url.clone(),
            temp_dir,
            job_state: Arc::new(Mutex::new(HashMap::new())),
            job_meta: Arc::new(Mutex::new(HashMap::new())),
            blurhash_cache,
            redis_backend,
            listener_state: Arc::new(Mutex::new(ListenerState {
                users: 0,
                task: None,
            })),
            upload_limiter: ConcurrencyLimiter::new(config.transfer_upload_concurrency),
            download_limiter: ConcurrencyLimiter::new(config.transfer_download_concurrency),
            repack_limiter: ConcurrencyLimiter::new(config.transfer_repack_concurrency),
            progress_push_interval: Duration::from_millis(250),
            config: Arc::new(config),
        }))
    }

    pub fn new_job_state(&self) -> JobState {
        new_job_state()
    }

    pub fn job_dir(&self, job_id: &str) -> std::io::Result<PathBuf> {
        safe_path(&self.temp_dir, job_id)
    }

    async fn local_clients(&self, job_id: &str) -> Vec<WsClientHandle> {
        let lock = self.job_state.lock().await;
        lock.get(job_id)
            .map(|state| state.clients.clone())
            .unwrap_or_default()
    }

    async fn close_local_clients(&self, job_id: &str) -> Vec<WsClientHandle> {
        let mut lock = self.job_state.lock().await;
        lock.get_mut(job_id)
            .map(|state| {
                let clients = state.clients.clone();
                state.clients.clear();
                clients
            })
            .unwrap_or_default()
    }

    async fn remove_local_job(&self, job_id: &str) -> Vec<WsClientHandle> {
        let mut lock = self.job_state.lock().await;
        let clients = lock
            .remove(job_id)
            .map(|state| state.clients)
            .unwrap_or_default();
        drop(lock);
        self.job_meta.lock().await.remove(job_id);
        clients
    }

    async fn local_job_state_snapshot(&self, job_id: &str) -> JobState {
        self.job_state
            .lock()
            .await
            .get(job_id)
            .cloned()
            .unwrap_or_else(new_job_state)
    }

    pub async fn local_job_ids(&self) -> Vec<String> {
        let lock = self.job_state.lock().await;
        let mut ids = lock.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        ids
    }

    pub async fn read_job_state(&self, job_id: &str) -> Option<JobState> {
        if let Some(redis) = &self.redis_backend {
            if let Some(mut remote_state) = redis.read_job_state(job_id).await {
                remote_state.clients = self.local_clients(job_id).await;
                let mut lock = self.job_state.lock().await;
                lock.insert(job_id.to_string(), remote_state.clone());
                return Some(remote_state);
            }
        }

        self.job_state.lock().await.get(job_id).cloned()
    }

    pub async fn save_job_state(&self, job_id: &str, state: Option<JobState>) -> JobState {
        let current = match state {
            Some(state) => state,
            None => self
                .job_state
                .lock()
                .await
                .get(job_id)
                .cloned()
                .unwrap_or_else(new_job_state),
        };
        let mut lock = self.job_state.lock().await;
        lock.insert(job_id.to_string(), current.clone());
        drop(lock);
        if let Some(redis) = &self.redis_backend {
            redis.write_job_state(job_id, &current).await;
        }
        current
    }

    pub async fn list_job_ids(&self) -> Vec<String> {
        if let Some(redis) = &self.redis_backend {
            if let Some(job_ids) = redis.list_job_ids().await {
                return job_ids;
            }
        }
        self.local_job_ids().await
    }

    pub async fn read_meta(&self, job_id: &str) -> Option<Value> {
        if let Some(redis) = &self.redis_backend {
            if let Some(remote_meta) = redis.read_meta(job_id).await {
                self.job_meta
                    .lock()
                    .await
                    .insert(job_id.to_string(), remote_meta.clone());
                return Some(remote_meta);
            }
        }
        self.job_meta.lock().await.get(job_id).cloned()
    }

    pub async fn write_meta(&self, job_id: &str, data: Value) {
        self.job_meta
            .lock()
            .await
            .insert(job_id.to_string(), data.clone());
        if let Some(redis) = &self.redis_backend {
            redis.write_meta(job_id, &data).await;
        }
    }

    pub async fn read_local_blurhash_cache(&self, cache_key: &str) -> Option<(String, u32, u32)> {
        let Some(cache) = &self.blurhash_cache else {
            return None;
        };
        let mut lock = cache.lock().await;
        lock.get(cache_key).cloned()
    }

    pub async fn write_local_blurhash_cache(&self, cache_key: &str, data: (String, u32, u32)) {
        let Some(cache) = &self.blurhash_cache else {
            return;
        };
        let mut lock = cache.lock().await;
        lock.put(cache_key.to_string(), data);
    }

    pub async fn read_blurhash_cache(&self, cache_key: &str) -> Option<Value> {
        let redis = self.redis_backend.as_ref()?;
        redis.read_blurhash_cache(cache_key).await
    }

    pub async fn write_blurhash_cache(&self, cache_key: &str, data: Value) {
        let Some(redis) = &self.redis_backend else {
            return;
        };
        redis
            .write_blurhash_cache(cache_key, &data, self.config.blurhash_cache_ttl_seconds)
            .await;
    }

    pub async fn build_state_event(
        &self,
        job_id: &str,
        event: &str,
        extra: Option<Map<String, Value>>,
    ) -> Value {
        let mut lock = self.job_state.lock().await;
        let state = lock.entry(job_id.to_string()).or_insert_with(new_job_state);
        state_event_payload(event, state, extra)
    }

    async fn deliver_local_message(&self, job_id: &str, message: &Value) {
        let clients = self.local_clients(job_id).await;
        if clients.is_empty() {
            return;
        }

        let mut dead_ids = Vec::new();
        for client in clients {
            if client
                .sender
                .send(Message::Text(message.to_string()))
                .is_err()
            {
                dead_ids.push(client.id);
            }
        }
        if dead_ids.is_empty() {
            return;
        }

        let mut lock = self.job_state.lock().await;
        if let Some(state) = lock.get_mut(job_id) {
            state
                .clients
                .retain(|client| !dead_ids.contains(&client.id));
        }
    }

    pub async fn broadcast(&self, job_id: &str, message: Value) {
        self.deliver_local_message(job_id, &message).await;
        if let Some(redis) = &self.redis_backend {
            let state = self.local_job_state_snapshot(job_id).await;
            redis.publish_event(job_id, &state, &message).await;
        }
    }

    pub async fn close_clients(&self, job_id: &str) {
        let clients = self.close_local_clients(job_id).await;
        for client in clients {
            let _ = client.sender.send(Message::Close(None));
        }
        if let Some(redis) = &self.redis_backend {
            redis.publish_close_clients(job_id).await;
        }
    }

    pub async fn set_state(&self, job_id: &str, updates: Map<String, Value>) {
        let _ = self.read_job_state(job_id).await;
        let snapshot = {
            let mut lock = self.job_state.lock().await;
            let state = lock.entry(job_id.to_string()).or_insert_with(new_job_state);
            if let Some(started) = updates.get("started").and_then(Value::as_bool) {
                state.started = started;
            }
            if let Some(status) = updates.get("status").and_then(Value::as_str) {
                state.status = status.to_string();
            }
            if let Some(stage) = updates.get("stage").and_then(Value::as_str) {
                state.stage = stage.to_string();
            }
            if let Some(bytes) = updates.get("bytes").and_then(Value::as_u64) {
                state.bytes = bytes;
            }
            if let Some(total) = updates.get("total") {
                state.total = total.as_u64();
            }
            if let Some(percent) = updates.get("percent") {
                state.percent = percent.as_u64();
            }
            if let Some(error) = updates.get("error") {
                state.error = error.as_str().map(|value| value.to_string());
            }
            state.last_activity = unix_time_seconds();
            state.clone()
        };
        let _ = self.save_job_state(job_id, Some(snapshot)).await;
    }

    pub async fn set_stage(&self, job_id: &str, stage: &str) {
        let _ = self.read_job_state(job_id).await;
        let (snapshot, payload) = {
            let mut lock = self.job_state.lock().await;
            let state = lock.entry(job_id.to_string()).or_insert_with(new_job_state);
            state.stage = stage.to_string();
            state.percent = None;
            state.last_activity = unix_time_seconds();
            let payload = state_event_payload("stage", state, None);
            (state.clone(), payload)
        };
        let _ = self.save_job_state(job_id, Some(snapshot)).await;
        self.broadcast(job_id, payload).await;
    }

    pub async fn delete_job_and_dir(&self, job_id: &str) {
        self.close_clients(job_id).await;
        let _ = self.remove_local_job(job_id).await;
        if let Some(redis) = &self.redis_backend {
            redis.delete_job(job_id).await;
            redis.publish_delete(job_id).await;
        }
        if let Ok(job_dir) = self.job_dir(job_id) {
            if job_dir.exists() {
                let _ = tokio::fs::remove_dir_all(job_dir).await;
            }
        }
    }

    pub async fn job_error_cleanup(&self, job_id: &str, reason: &str) {
        tracing::info!(job_id, reason, "job error cleanup");
        self.delete_job_and_dir(job_id).await;
    }

    pub async fn start_job_event_listener(self: &Arc<Self>) {
        let mut listener_state = self.listener_state.lock().await;
        listener_state.users += 1;

        let Some(redis) = self.redis_backend.clone() else {
            return;
        };

        if let Some(task) = listener_state.task.as_ref() {
            if !task.is_finished() {
                return;
            }
        }

        let state = Arc::clone(self);
        let callback: RedisEventCallback = Arc::new(move |payload: Value| {
            let state = Arc::clone(&state);
            async move {
                state.handle_remote_event(payload).await;
            }
            .boxed()
        });

        listener_state.task = Some(tokio::spawn(async move {
            redis.listen(callback).await;
        }));
    }

    pub async fn stop_job_event_listener(self: &Arc<Self>) {
        let task = {
            let mut listener_state = self.listener_state.lock().await;
            if listener_state.users > 0 {
                listener_state.users -= 1;
            }
            if listener_state.users == 0 {
                listener_state.task.take()
            } else {
                None
            }
        };

        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }

    async fn handle_remote_event(self: Arc<Self>, payload: Value) {
        let Some(redis) = &self.redis_backend else {
            return;
        };
        if payload.get("origin_id").and_then(Value::as_str) == Some(redis.instance_id()) {
            return;
        }

        let Some(job_id) = payload
            .get("job_id")
            .and_then(Value::as_str)
            .map(|value| value.to_string())
        else {
            return;
        };

        let Some(kind) = payload.get("kind").and_then(Value::as_str) else {
            return;
        };

        match kind {
            "event" => {
                if let Some(state) = payload.get("state").and_then(Value::as_object) {
                    if let Ok(mut remote_state) =
                        serde_json::from_value::<JobState>(Value::Object(state.clone()))
                    {
                        remote_state.clients = self.local_clients(&job_id).await;
                        let mut lock = self.job_state.lock().await;
                        lock.insert(job_id.clone(), remote_state);
                    }
                }
                if let Some(message) = payload.get("message") {
                    if message.is_object() {
                        self.deliver_local_message(&job_id, message).await;
                    }
                }
            }
            "close_clients" => {
                let clients = self.close_local_clients(&job_id).await;
                for client in clients {
                    let _ = client.sender.send(Message::Close(None));
                }
            }
            "delete" => {
                let clients = self.remove_local_job(&job_id).await;
                for client in clients {
                    let _ = client.sender.send(Message::Close(None));
                }
            }
            _ => {}
        }
    }

    pub fn transfer_callback_url(&self) -> String {
        self.config
            .manager_transfer_callback_url
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                format!(
                    "{}/internal/storage/transfer-completions",
                    self.manager_url.trim_end_matches('/')
                )
            })
    }

    pub fn transfer_jwt_ttl_seconds(&self) -> u64 {
        self.config.transfer_callback_ttl_seconds
    }

    pub async fn notify_manager(&self, payload: Map<String, Value>) {
        let job_id = payload
            .get("job_id")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let status = payload
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let reason = payload
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let transfer_kind = payload
            .get("transfer_kind")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let callback_action = payload
            .get("callback_action")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let mode = payload
            .get("mode")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let condition = payload
            .get("condition")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        tracing::info!(
            job_id = %job_id,
            status = %status,
            reason = %reason,
            transfer_kind = %transfer_kind,
            callback_action = %callback_action,
            mode = %mode,
            condition = %condition,
            "sending transfer callback"
        );
        let Some(token) = auth::encode_transfer_jwt(
            &self.config,
            &payload,
            "manager",
            self.transfer_jwt_ttl_seconds(),
        ) else {
            return;
        };
        let url = self.transfer_callback_url();
        let body = match serde_json::to_vec(&Value::Object(payload)) {
            Ok(bytes) => bytes,
            Err(_) => return,
        };
        let uri: Uri = match url.parse() {
            Ok(uri) => uri,
            Err(_) => return,
        };
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build(connector);
        let request = match Request::post(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body)))
        {
            Ok(request) => request,
            Err(_) => return,
        };
        let timeout = Duration::from_secs(
            self.config
                .transfer_callback_timeout_seconds
                .unwrap_or(30.0) as u64,
        );
        match tokio::time::timeout(timeout, client.request(request)).await {
            Ok(Ok(response)) => {
                tracing::info!(
                    job_id = %job_id,
                    status_code = %response.status(),
                    "transfer callback delivered"
                );
            }
            Ok(Err(err)) => {
                tracing::warn!(
                    job_id = %job_id,
                    error = %err,
                    "transfer callback request failed"
                );
            }
            Err(_) => {
                tracing::warn!(
                    job_id = %job_id,
                    timeout_seconds = timeout.as_secs_f64(),
                    "transfer callback timed out"
                );
            }
        }
    }
}
