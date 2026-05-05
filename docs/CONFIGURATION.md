# Configuration

The application reads configuration directly from environment variables. There is no Python config file
anymore.

## Core Settings

| Env var | Required | Description |
| --- | --- | --- |
| `MAIN_DIR` | yes | Root directory for permanent and temporary storage |
| `MANAGER_URL` | yes | Base Manager API URL used for fallback transfer callback URL, without a path suffix |
| `ACCESS_SERVICE_URL` | yes | Base access service URL used for protected mod download checks |
| `MANAGER_TRANSFER_CALLBACK_URL` | no | Explicit callback endpoint; if empty, fallback is `<MANAGER_URL>/internal/storage/transfer-completions` |
| `TRANSFER_JWT_SECRET` | yes | Shared secret for transfer JWT validation and Manager callback signing |
| `REDIS_URL` | no | Redis connection URL used for shared job state, metadata, and websocket fan-out; leave empty for local in-memory fallback |
| `REDIS_PREFIX` | no | Key prefix used for Redis job keys and pub/sub channels; default `open-workshop-storage` |
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
| `BLURHASH_CACHE_SIZE` | no | In-memory LRU cache size for computed BlurHash entries; default `4096`. Each worker keeps its own cache, so higher values can materially increase RSS. |
| `BLURHASH_CACHE_TTL_SECONDS` | no | Redis TTL for shared BlurHash cache entries when `REDIS_URL` is configured; default `604800`, `0` disables expiry |

### Service Ownership

- Distributor service: `MAIN_DIR`, `ACCESS_SERVICE_URL`, `ACCESS_SERVICE_TIMEOUT_SECONDS`, `BLURHASH_CACHE_SIZE`, `BLURHASH_CACHE_TTL_SECONDS`, `REDIS_URL`, `REDIS_PREFIX` when using shared BlurHash cache. Keep `BLURHASH_CACHE_SIZE` conservative unless you have a small number of hot images or plenty of per-worker memory headroom.
- Loader service: `MAIN_DIR`, `MANAGER_URL`, `TRANSFER_JWT_SECRET`, `REDIS_*`, `TRANSFER_*`, cleanup settings, and internal tokens

## Cleanup Settings

| Env var | Default | Description |
| --- | --- | --- |
| `CLEANUP_INTERVAL_SECONDS` | `60` | How often cleanup loop scans inactive jobs |
| `JOB_TTL_SECONDS` | `10800` | How long an inactive job can remain before cleanup removes it |

## Token Settings

| Env var | Used by | Description |
| --- | --- | --- |
| `DELETE_FILE` | Manager -> storage | Secret used by `/delete` |
| `UPLOAD_FILE` | Manager -> storage | Secret used by `/upload` |
| `STORAGE_MANAGE_TOKEN` | Manager -> storage | Secret used by `/transfer/repack` and `/transfer/move` |

Incoming form tokens are validated with bcrypt in the storage service.

## Token Generation

Use:

```bash
cargo run --bin token_gen
```

This helper prints generated token pairs. Review the output carefully and place the hashes into the
corresponding environment variables.

## Telemetry Settings

Telemetry is optional and can be set through environment variables.

| Env var | Description |
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

Redis is optional for tests and local development, but it is the recommended backend for production and any
multi-worker loader deployment.

## Required External Dependency

`7z` must be available in `PATH`.

Ubuntu / Debian:

```bash
sudo apt install -y p7zip-full
```

## Minimal Example

```bash
export MAIN_DIR=storage
export MANAGER_URL=http://127.0.0.1:7776
export ACCESS_SERVICE_URL=http://127.0.0.1:7777
export MANAGER_TRANSFER_CALLBACK_URL=
export TRANSFER_JWT_SECRET=replace-me
export REDIS_URL=redis://127.0.0.1:6379/0
export REDIS_PREFIX=open-workshop-storage
export TRANSFER_CALLBACK_TTL_SECONDS=600
export TRANSFER_MAX_BYTES=0
export TRANSFER_MAX_UNPACKED_BYTES=0
export TRANSFER_UPLOAD_CONCURRENCY=8
export TRANSFER_DOWNLOAD_CONCURRENCY=16
export TRANSFER_REPACK_CONCURRENCY=8
export TRANSFER_UPLOAD_TIMEOUT_SECONDS=3600
export TRANSFER_DOWNLOAD_TIMEOUT_SECONDS=3600
export TRANSFER_CALLBACK_TIMEOUT_SECONDS=30
export SEVEN_ZIP_TIMEOUT_SECONDS=3600
export SEVEN_ZIP_IDLE_TIMEOUT_SECONDS=60
export ACCESS_SERVICE_TIMEOUT_SECONDS=30
export BLURHASH_CACHE_SIZE=4096
export BLURHASH_CACHE_TTL_SECONDS=604800
export CLEANUP_INTERVAL_SECONDS=60
export JOB_TTL_SECONDS=10800
export DELETE_FILE=replace-with-bcrypt-hash
export UPLOAD_FILE=replace-with-bcrypt-hash
export STORAGE_MANAGE_TOKEN=replace-with-bcrypt-hash
```
