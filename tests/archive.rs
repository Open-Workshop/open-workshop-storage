use std::collections::HashMap;

use open_workshop_storage::archive::{
    archive_entries_packed_bytes, archive_entries_unpacked_bytes, zip_uses_deflated_or_better,
};

fn entry(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[test]
fn allows_mixed_store_and_deflate_when_total_savings_is_meaningful() {
    let entries = [
        entry(&[("Type", "zip")]),
        entry(&[
            ("Path", "archive/random.bin"),
            ("Folder", "-"),
            ("Size", "100000"),
            ("Packed Size", "100000"),
            ("Method", "Store"),
        ]),
        entry(&[
            ("Path", "archive/text.txt"),
            ("Folder", "-"),
            ("Size", "100000"),
            ("Packed Size", "341"),
            ("Method", "Deflate"),
        ]),
    ];

    assert!(zip_uses_deflated_or_better(Some(&entries)));
}

#[test]
fn rejects_zip_when_total_savings_is_below_threshold() {
    let entries = [
        entry(&[("Type", "zip")]),
        entry(&[
            ("Path", "archive/a.bin"),
            ("Folder", "-"),
            ("Size", "100000"),
            ("Packed Size", "99550"),
            ("Method", "Deflate"),
        ]),
        entry(&[
            ("Path", "archive/b.bin"),
            ("Folder", "-"),
            ("Size", "100000"),
            ("Packed Size", "99550"),
            ("Method", "Store"),
        ]),
    ];

    assert!(!zip_uses_deflated_or_better(Some(&entries)));
}

#[test]
fn computes_archive_byte_totals() {
    let entries = [
        entry(&[("Type", "zip")]),
        entry(&[
            ("Path", "archive/a.bin"),
            ("Folder", "-"),
            ("Size", "10"),
            ("Packed Size", "6"),
            ("Method", "Deflate"),
        ]),
        entry(&[
            ("Path", "archive/b.bin"),
            ("Folder", "-"),
            ("Size", "20"),
            ("Packed Size", "12"),
            ("Method", "Store"),
        ]),
    ];

    assert_eq!(archive_entries_unpacked_bytes(Some(&entries)), Some(30));
    assert_eq!(archive_entries_packed_bytes(Some(&entries)), Some(18));
}
