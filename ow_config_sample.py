
MAIN_DIR = 'storage'
MANAGER_URL = 'http://127.0.0.1:7776'
ACCESS_SERVICE_URL = 'http://127.0.0.1:7777'
MANAGER_TRANSFER_CALLBACK_URL = ''
TRANSFER_JWT_SECRET = ''
REDIS_URL = ''
REDIS_PREFIX = 'open-workshop-storage'
TRANSFER_CALLBACK_TTL_SECONDS = 600
TRANSFER_MAX_BYTES = 0
TRANSFER_MAX_UNPACKED_BYTES = 0
TRANSFER_UPLOAD_CONCURRENCY = 8
TRANSFER_DOWNLOAD_CONCURRENCY = 16
TRANSFER_REPACK_CONCURRENCY = 8
TRANSFER_UPLOAD_TIMEOUT_SECONDS = 3600
TRANSFER_DOWNLOAD_TIMEOUT_SECONDS = 3600
TRANSFER_CALLBACK_TIMEOUT_SECONDS = 30
SEVEN_ZIP_TIMEOUT_SECONDS = 3600
SEVEN_ZIP_IDLE_TIMEOUT_SECONDS = 60
ACCESS_SERVICE_TIMEOUT_SECONDS = 30
BLURHASH_CACHE_SIZE = 100000
BLURHASH_CACHE_TTL_SECONDS = 604800

# Cleanup settings
CLEANUP_INTERVAL_SECONDS = 60  # Интервал между проверками очистки
JOB_TTL_SECONDS = 10800  # Время неактивности перед удалением job (3 часа)

# Токены

## Отправляемые
delete_file = ''
upload_file = ''
storage_manage_token = ''


# Optional telemetry settings (recommended to set via environment variables)
# UPTRACE_DSN = "https://<token>@api.uptrace.dev/<project_id>"
# OTEL_SERVICE_NAME = "open-workshop-storage"
# OTEL_SERVICE_VERSION = "1.0.0"
# OTEL_DEPLOYMENT_ENVIRONMENT = "production"
# UPTRACE_OTLP_PROTOCOL = "grpc"  # or "http"
# UPTRACE_FASTAPI_EXCLUDED_URLS = "^.*/docs$,^.*/openapi\\.json$,^/favicon\\.ico$,^/robots\\.txt$"
# UPTRACE_FASTAPI_EXCLUDE_SPANS = "receive,send"  # hide noisy ASGI internal spans
# UPTRACE_OTLP_TRACES_URL = "https://api.uptrace.dev/v1/traces"
# UPTRACE_OTLP_GRPC_URL = "https://api.uptrace.dev:4317"
