use open_workshop_storage::blurhash_cache::build_blurhash_cache_key;

#[test]
fn redis_cache_keys_remain_compatible_across_sha_upgrade() {
    let key = build_blurhash_cache_key("/srv/mods/42/logo.webp", 123456789, 4321, 4, 3, 64);
    assert_eq!(key, "8bccffc09ced488c53912b2e57b1617449fdbb0fcfc5c367a5ad0264ababda7a");
}
