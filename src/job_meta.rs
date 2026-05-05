use std::sync::Arc;

use serde_json::{Map, Value};

use crate::runtime::AppState;

pub async fn update_job_meta(
    state: &Arc<AppState>,
    job_id: &str,
    updates: Map<String, Value>,
    warning_message: &str,
) {
    let Some(mut meta) = state.read_meta(job_id).await else {
        eprintln!("{} job_id={}", warning_message.replace("%s", "{}"), job_id);
        return;
    };
    if let Some(map) = meta.as_object_mut() {
        for (key, value) in updates {
            map.insert(key, value);
        }
        state.write_meta(job_id, meta).await;
        return;
    }
    eprintln!("{} job_id={}", warning_message.replace("%s", "{}"), job_id);
}
