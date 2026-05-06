use std::env;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub main_dir: PathBuf,
    pub manager_url: String,
    pub access_service_url: String,
    pub manager_transfer_callback_url: Option<String>,
    pub transfer_jwt_secret: Option<String>,
    pub redis_url: Option<String>,
    pub redis_prefix: String,
    pub transfer_callback_ttl_seconds: u64,
    pub transfer_max_bytes: Option<u64>,
    pub transfer_max_unpacked_bytes: Option<u64>,
    pub transfer_upload_concurrency: usize,
    pub transfer_download_concurrency: usize,
    pub transfer_repack_concurrency: usize,
    pub transfer_upload_timeout_seconds: Option<f64>,
    pub transfer_download_timeout_seconds: Option<f64>,
    pub transfer_callback_timeout_seconds: Option<f64>,
    pub seven_zip_timeout_seconds: Option<f64>,
    pub seven_zip_idle_timeout_seconds: Option<f64>,
    pub access_service_timeout_seconds: u64,
    pub blurhash_cache_size: usize,
    pub blurhash_cache_ttl_seconds: u64,
    pub cleanup_interval_seconds: u64,
    pub job_ttl_seconds: u64,
    pub delete_file: Option<String>,
    pub upload_file: Option<String>,
    pub storage_manage_token: Option<String>,
}

impl AppConfig {
    pub fn load() -> Self {
        Self::from_env()
    }

    fn from_env() -> Self {
        let main_dir = read_env_string("MAIN_DIR", "storage");
        let manager_url = read_env_string("MANAGER_URL", "http://127.0.0.1:7776");
        let access_service_url = read_env_string("ACCESS_SERVICE_URL", "http://127.0.0.1:7777");
        let manager_transfer_callback_url =
            read_env_optional_string("MANAGER_TRANSFER_CALLBACK_URL");
        let transfer_jwt_secret = read_env_optional_string("TRANSFER_JWT_SECRET");
        let redis_url = read_env_optional_string("REDIS_URL");
        let redis_prefix = read_env_string("REDIS_PREFIX", "open-workshop-storage");
        let transfer_callback_ttl_seconds = read_env_u64("TRANSFER_CALLBACK_TTL_SECONDS", 600);
        let transfer_max_bytes = read_env_optional_u64_nonzero("TRANSFER_MAX_BYTES");
        let transfer_max_unpacked_bytes =
            read_env_optional_u64_nonzero("TRANSFER_MAX_UNPACKED_BYTES");
        let transfer_upload_concurrency = read_env_usize("TRANSFER_UPLOAD_CONCURRENCY", 8);
        let transfer_download_concurrency = read_env_usize("TRANSFER_DOWNLOAD_CONCURRENCY", 16);
        let transfer_repack_concurrency = read_env_usize("TRANSFER_REPACK_CONCURRENCY", 8);
        let transfer_upload_timeout_seconds =
            read_env_optional_f64_positive("TRANSFER_UPLOAD_TIMEOUT_SECONDS");
        let transfer_download_timeout_seconds =
            read_env_optional_f64_positive("TRANSFER_DOWNLOAD_TIMEOUT_SECONDS");
        let transfer_callback_timeout_seconds =
            read_env_optional_f64_positive("TRANSFER_CALLBACK_TIMEOUT_SECONDS");
        let seven_zip_timeout_seconds = read_env_optional_f64_positive("SEVEN_ZIP_TIMEOUT_SECONDS");
        let seven_zip_idle_timeout_seconds =
            read_env_optional_f64_positive("SEVEN_ZIP_IDLE_TIMEOUT_SECONDS");
        let access_service_timeout_seconds = read_env_u64("ACCESS_SERVICE_TIMEOUT_SECONDS", 30);
        let blurhash_cache_size = read_env_usize("BLURHASH_CACHE_SIZE", 4096);
        let blurhash_cache_ttl_seconds = read_env_u64("BLURHASH_CACHE_TTL_SECONDS", 604800);
        let cleanup_interval_seconds = read_env_u64("CLEANUP_INTERVAL_SECONDS", 60);
        let job_ttl_seconds = read_env_u64("JOB_TTL_SECONDS", 10800);
        let delete_file = read_env_optional_string_any(&["DELETE_FILE", "delete_file"]);
        let upload_file = read_env_optional_string_any(&["UPLOAD_FILE", "upload_file"]);
        let storage_manage_token =
            read_env_optional_string_any(&["STORAGE_MANAGE_TOKEN", "storage_manage_token"]);

        Self {
            main_dir: PathBuf::from(main_dir),
            manager_url,
            access_service_url,
            manager_transfer_callback_url,
            transfer_jwt_secret,
            redis_url,
            redis_prefix,
            transfer_callback_ttl_seconds,
            transfer_max_bytes,
            transfer_max_unpacked_bytes,
            transfer_upload_concurrency,
            transfer_download_concurrency,
            transfer_repack_concurrency,
            transfer_upload_timeout_seconds,
            transfer_download_timeout_seconds,
            transfer_callback_timeout_seconds,
            seven_zip_timeout_seconds,
            seven_zip_idle_timeout_seconds,
            access_service_timeout_seconds,
            blurhash_cache_size,
            blurhash_cache_ttl_seconds,
            cleanup_interval_seconds,
            job_ttl_seconds,
            delete_file,
            upload_file,
            storage_manage_token,
        }
    }

    pub fn token_hash(&self, name: &str) -> Option<&str> {
        match name {
            "delete_file" => self.delete_file.as_deref(),
            "upload_file" => self.upload_file.as_deref(),
            "storage_manage_token" => self.storage_manage_token.as_deref(),
            _ => None,
        }
    }
}

fn read_env_value(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|value| {
        let value = value.trim().to_string();
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    })
}

fn read_env_value_any(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| read_env_value(key))
}

fn read_env_string(key: &str, default: &str) -> String {
    read_env_value(key).unwrap_or_else(|| default.to_string())
}

fn read_env_optional_string(key: &str) -> Option<String> {
    read_env_value(key)
}

fn read_env_optional_string_any(keys: &[&str]) -> Option<String> {
    read_env_value_any(keys)
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    read_env_value(key)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn read_env_optional_u64_nonzero(key: &str) -> Option<u64> {
    read_env_value(key)
        .and_then(|value| value.parse::<i64>().ok())
        .and_then(|value| if value > 0 { Some(value as u64) } else { None })
}

fn read_env_usize(key: &str, default: usize) -> usize {
    read_env_value(key)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn read_env_optional_f64_positive(key: &str) -> Option<f64> {
    read_env_value(key)
        .and_then(|value| value.parse::<f64>().ok())
        .and_then(|value| if value > 0.0 { Some(value) } else { None })
}
