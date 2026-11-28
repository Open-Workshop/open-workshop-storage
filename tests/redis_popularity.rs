use open_workshop_storage::redis_store::{BlurhashPopularityItem, RedisBackend};
use std::time::{SystemTime, UNIX_EPOCH};

// Set REDIS_TEST_URL to run against an isolated Redis instance.
#[tokio::test]
async fn popular_entries_are_renewed_or_restored_without_overwriting() {
    let Ok(url) = std::env::var("REDIS_TEST_URL") else { return; };
    let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let prefix = format!("ow-popularity-test-{unique}");
    let backend = RedisBackend::new(&url, &prefix).unwrap();
    let client = redis::Client::open(url).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let live = format!("{prefix}:blurhash:live");
    let missing = format!("{prefix}:blurhash:missing");
    let _: String = redis::cmd("SET").arg(&live).arg("existing").arg("EX").arg(20)
        .query_async(&mut connection).await.unwrap();

    let items = [
        BlurhashPopularityItem { key: "live".into(), value: ("new-value".into(), 3, 4), hits: 16 },
        BlurhashPopularityItem { key: "missing".into(), value: ("restored".into(), 6, 4), hits: 4 },
    ];
    let result = backend.update_blurhash_popularity(&items, 60).await.unwrap();
    assert_eq!((result.forwarded, result.restored, result.renewed), (20, 1, 1));
    let value: String = redis::cmd("GET").arg(&live).query_async(&mut connection).await.unwrap();
    let ttl: i64 = redis::cmd("TTL").arg(&live).query_async(&mut connection).await.unwrap();
    assert_eq!(value, "existing");
    assert!(ttl > 45);
    let restored: String = redis::cmd("GET").arg(&missing).query_async(&mut connection).await.unwrap();
    let restored: serde_json::Value = serde_json::from_str(&restored).unwrap();
    assert_eq!(restored["blurhash"], "restored");
    let _: i64 = redis::cmd("DEL").arg(&live).arg(&missing).query_async(&mut connection).await.unwrap();
}
