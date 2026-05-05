use bcrypt::hash;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

const TOKENS: [&str; 3] = ["delete_file", "upload_file", "storage_manage_token"];

fn generate_token(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

fn hash_token(token: &str) -> String {
    hash(token, 12).expect("bcrypt hash")
}

fn main() {
    println!("Generating storage tokens");
    println!("{}", "-".repeat(50));

    for token_name in TOKENS {
        let plain = generate_token(32);
        let hashed = hash_token(&plain);
        println!();
        println!("{token_name}:");
        println!("  plain: {plain}");
        println!("  hashed: {hashed}");
    }

    println!();
    println!("{}", "=".repeat(50));
    println!(
        "Keep the plain tokens safe and store only the hashes in your deployment environment."
    );
}
