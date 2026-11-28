use bcrypt::hash;
use rand::distr::{Alphanumeric, SampleString};

const TOKENS: [&str; 3] = ["delete_file", "upload_file", "storage_manage_token"];

fn generate_token(length: usize) -> String {
    Alphanumeric.sample_string(&mut rand::rng(), length)
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

#[cfg(test)]
mod tests {
    use super::generate_token;

    #[test]
    fn generates_expected_length_alphanumeric_tokens() {
        let a = generate_token(32);
        let b = generate_token(32);
        assert_eq!(a.len(), 32);
        assert!(a.bytes().all(|c| c.is_ascii_alphanumeric()));
        assert_ne!(a, b);
    }
}
