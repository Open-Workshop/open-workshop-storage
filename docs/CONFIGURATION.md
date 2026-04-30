# Configuration

## Config File

The application reads Python settings from `ow_config.py`.

Start from:

```bash
cp ow_config_sample.py ow_config.py
```

## Core Settings

| Setting | Required | Description |
| --- | --- | --- |
| `MAIN_DIR` | yes | Root directory for permanent and temporary storage |
| `MANAGER_URL` | yes | Base Manager API URL used for fallback transfer callback URL, without a path suffix |
| `ACCESS_SERVICE_URL` | yes | Base access service URL used for protected mod download checks |
| `MANAGER_TRANSFER_CALLBACK_URL` | no | Explicit callback endpoint; if empty, fallback is `<MANAGER_URL>/internal/storage/transfer-completions` |
| `TRANSFER_JWT_SECRET` | yes | Shared secret for transfer JWT validation and Manager callback signing |
| `TRANSFER_CALLBACK_TTL_SECONDS` | no | Lifetime of outgoing callback JWTs; default `600` |
| `TRANSFER_MAX_BYTES` | no | Global maximum transfer size; `0` disables the limit |
| `TRANSFER_MAX_UNPACKED_BYTES` | no | Global maximum unpacked archive size; `0` disables the limit |
| `TRANSFER_UPLOAD_CONCURRENCY` | no | Maximum raw transfer uploads processed at once; default `8`, `0` disables the limiter |
| `TRANSFER_DOWNLOAD_CONCURRENCY` | no | Maximum URL transfer downloads processed at once; default `16`, `0` disables the limiter |
| `TRANSFER_REPACK_CONCURRENCY` | no | Maximum archive repack operations processed at once; default `8`, `0` disables the limiter |
| `TRANSFER_UPLOAD_TIMEOUT_SECONDS` | no | Total timeout for raw transfer uploads, including processing; default `3600`, `0` disables it |
| `TRANSFER_DOWNLOAD_TIMEOUT_SECONDS` | no | Total timeout for URL transfer downloads; default `3600`, `0` disables it |
| `TRANSFER_CALLBACK_TIMEOUT_SECONDS` | no | Total timeout for Manager transfer callbacks; default `30`, `0` disables it |
| `SEVEN_ZIP_TIMEOUT_SECONDS` | no | Total timeout for each `7z` command; default `3600`, `0` disables it |
| `SEVEN_ZIP_IDLE_TIMEOUT_SECONDS` | no | Idle timeout while waiting for the next `7z` output chunk; default `60`, `0` disables it |
| `ACCESS_SERVICE_TIMEOUT_SECONDS` | yes | Timeout for access-service download checks |
| `BLURHASH_CACHE_SIZE` | no | In-memory LRU cache size for computed BlurHash entries; default `100000` |

## Cleanup Settings

| Setting | Default | Description |
| --- | --- | --- |
| `CLEANUP_INTERVAL_SECONDS` | `60` | How often cleanup loop scans inactive jobs |
| `JOB_TTL_SECONDS` | `10800` | How long an inactive job can remain before cleanup removes it |

## Token Settings

| Setting | Used by | Description |
| --- | --- | --- |
| `delete_file` | Manager -> storage | Secret used by `/delete` |
| `upload_file` | Manager -> storage | Secret used by `/upload` |
| `storage_manage_token` | Manager -> storage | Secret used by `/transfer/repack` and `/transfer/move` |

### Important note

Incoming form tokens are validated with bcrypt in the storage service.

## Token Generation

Use:

```bash
python token_gen.py
```

This helper prints generated token pairs. Review the output carefully and place values into `ow_config.py`
according to your integration contract.

## Telemetry Settings

Telemetry is optional and can be set through environment variables or `ow_config.py`.

| Setting | Description |
| --- | --- |
| `UPTRACE_DSN` | Enables Uptrace / OpenTelemetry export |
| `OTEL_SERVICE_NAME` | Service name in traces |
| `OTEL_SERVICE_VERSION` | Service version in traces |
| `OTEL_DEPLOYMENT_ENVIRONMENT` | Environment name |
| `UPTRACE_OTLP_PROTOCOL` | `grpc` or `http` |
| `UPTRACE_FASTAPI_EXCLUDED_URLS` | Regex list of paths excluded from tracing |
| `UPTRACE_FASTAPI_EXCLUDE_SPANS` | Usually `receive,send` to hide noisy ASGI spans |
| `UPTRACE_OTLP_TRACES_URL` | Custom OTLP HTTP traces endpoint |
| `UPTRACE_OTLP_GRPC_URL` | Custom OTLP gRPC endpoint |

## Filesystem Expectations

The service writes under `MAIN_DIR`:

- `archive/`
- `resource/`
- `avatar/`
- `temp/`

Ensure the process user can create directories, write files, and remove old job directories.

## Required External Dependency

`7z` must be available in `PATH`.

Ubuntu / Debian:

```bash
sudo apt install -y p7zip-full
```

## Minimal Example

```python
MAIN_DIR = "storage"
MANAGER_URL = "http://127.0.0.1:7776"
ACCESS_SERVICE_URL = "http://127.0.0.1:7777"
MANAGER_TRANSFER_CALLBACK_URL = ""
TRANSFER_JWT_SECRET = "replace-me"
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

CLEANUP_INTERVAL_SECONDS = 60
JOB_TTL_SECONDS = 10800

delete_file = "replace-with-bcrypt-hash"
upload_file = "replace-with-bcrypt-hash"
storage_manage_token = "replace-with-bcrypt-hash"
```
