mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use open_workshop_storage::config::AppConfig;
use open_workshop_storage::runtime::AppState;
use serde_json::json;
use tokio::time::{sleep, timeout};

use common::{temp_dir, test_config, test_state_with_config};

fn redis_config(root: &std::path::Path, redis_url: String, redis_prefix: String) -> AppConfig {
    let mut config = test_config(root);
    config.redis_url = Some(redis_url);
    config.redis_prefix = redis_prefix;
    config.blurhash_cache_size = 8;
    config
}

async fn wait_for_local_stage(state: &std::sync::Arc<AppState>, job_id: &str, stage: &str) {
    timeout(Duration::from_secs(5), async {
        loop {
            let observed = {
                let lock = state.job_state.lock().await;
                lock.get(job_id).map(|snapshot| snapshot.stage.clone())
            };
            if observed.as_deref() == Some(stage) {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("redis event was not observed in time");
}

#[tokio::test]
async fn redis_state_and_events_are_shared_between_instances() {
    let Ok(redis_url) = std::env::var("REDIS_URL") else {
        return;
    };

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let prefix = format!("open-workshop-storage-test-{}-{stamp}", std::process::id());
    let job_id = format!("redis-parity-{stamp}");
    let cache_key = format!("{job_id}:blurhash");

    let writer_root = temp_dir("redis-parity-writer");
    let reader_root = temp_dir("redis-parity-reader");
    let writer = test_state_with_config(redis_config(
        writer_root.path(),
        redis_url.clone(),
        prefix.clone(),
    ));
    let reader = test_state_with_config(redis_config(reader_root.path(), redis_url, prefix));

    reader.start_job_event_listener().await;
    writer.set_stage(&job_id, "uploading").await;
    wait_for_local_stage(&reader, &job_id, "uploading").await;

    let local_stage = {
        let lock = reader.job_state.lock().await;
        lock.get(&job_id).map(|snapshot| snapshot.stage.clone())
    };
    assert_eq!(local_stage.as_deref(), Some("uploading"));
    assert!(reader.list_job_ids().await.contains(&job_id));

    let meta = json!({ "job_id": job_id.clone(), "source": "redis" });
    writer.write_meta(&job_id, meta.clone()).await;
    assert_eq!(reader.read_meta(&job_id).await, Some(meta));

    let blurhash = json!({
        "blurhash": "LEHV6nWB2yk8pyo0adR*.7kCMdnj",
        "width": 6,
        "height": 4,
    });
    writer
        .write_blurhash_cache(&cache_key, blurhash.clone())
        .await;
    assert_eq!(reader.read_blurhash_cache(&cache_key).await, Some(blurhash));

    reader.stop_job_event_listener().await;
}
