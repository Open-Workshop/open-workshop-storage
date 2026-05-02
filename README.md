# Open Workshop Storage

[![imports-isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![BlackCode](https://img.shields.io/badge/code%20style-black-black)](https://github.com/psf/black)
[![mypy](https://img.shields.io/badge/type--checked-mypy-blue?logo=python)](https://mypy.readthedocs.io/en/stable/index.html)
[![Discord](https://img.shields.io/discord/792572437292253224?label=Discord&labelColor=%232c2f33&color=%237289da)](https://discord.gg/UnJnGHNbBp)
[![Telegram](https://img.shields.io/badge/Telegram-24A1DE)](https://t.me/miskler_dev)

Open Workshop Storage is split into two outward-facing services that share the same storage root and helper code:

- `distributor` serves stored files and blurhash metadata
- `loader` ingests uploads, runs transfer jobs, repacks artifacts, and reports completion back to Manager

The loader side now keeps active transfer state and websocket fan-out behind Redis, so it can run with
multiple workers when `REDIS_URL` is configured. If Redis is omitted, the project falls back to local
in-memory state for development and tests. The distributor side is stateless apart from its in-memory
BlurHash LRU cache, and when Redis is configured it also shares BlurHash results across workers with a
TTL-based cache entry. That lets the distributor scale independently without recomputing the same image
hashes in each process.

When both services are published on the same domain, only the conflicting control endpoints need separate
prefixes. In practice that means docs and health URLs live under `/distributor/...` and `/loader/...`,
while business routes like `/download/...`, `/upload`, and `/transfer/...` stay at their natural paths.

## Highlights

- FastAPI applications served with Granian.
- Separate loader and distributor entrypoints for clearer service boundaries.
- Loader runtime designed around Redis-backed job state and WebSocket progress fan-out.
- Protected archive downloads with access-service validation.
- Transfer pipeline for remote downloads and direct raw-body uploads.
- Archive repacking with `7z`, encrypted ZIP rejection, and unpacked-size heuristics.
- Automatic image normalization to WebP.
- WebSocket progress stream for upload, download, extract, and repack stages.
- Optional Uptrace / OpenTelemetry instrumentation.

## Quick Start

### 1. Install system dependency

Ubuntu / Debian:

```bash
sudo apt update
sudo apt install -y p7zip-full
```

### 2. Install Python dependencies

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

### 3. Create local config

```bash
cp ow_config_sample.py ow_config.py
```

Then fill at least:

- `MAIN_DIR`
- `MANAGER_URL`
- `ACCESS_SERVICE_URL`
- `TRANSFER_JWT_SECRET`
- `REDIS_URL` if you want shared loader state, shared BlurHash cache, or multiple workers
- token values in `ow_config.py`

Configuration details: [docs/CONFIGURATION.md](docs/CONFIGURATION.md)

### 4. Generate tokens

```bash
./.venv/bin/python token_gen.py
```

### 5. Start the services

Distributor:

```bash
granian --working-dir src --interface asgi --host 127.0.0.1 --port 8000 open_workshop_storage.distributor:app
```

Loader:

```bash
granian --working-dir src --interface asgi --host 127.0.0.1 --port 8001 --respawn-failed-workers --access-log open_workshop_storage.loader:app
```

The loader service can run with multiple workers when Redis is configured. Without Redis it falls back to
single-process in-memory state, which is still useful for local development and test runs. Production runs
can add `--workers N` as needed and should keep `--respawn-failed-workers` enabled so Granian replaces a
worker that exits unexpectedly.

## Watchdog

If you want a tiny external health monitor, the repository ships with [`watchdog.py`](watchdog.py).
It checks a health endpoint every 20 seconds by default and restarts the service after 5 minutes of
continuous failure.

Required env vars:

- `WATCHDOG_HEALTH_URL` - service health endpoint, for example `https://example.com/loader/healthz`
- `WATCHDOG_RESTART_COMMAND` - shell command used to restart the service, for example `systemctl restart open-workshop-storage`

Optional env vars:

- `WATCHDOG_CHECK_INTERVAL_SECONDS` - default `20`
- `WATCHDOG_RESTART_AFTER_SECONDS` - default `300`
- `WATCHDOG_REQUEST_TIMEOUT_SECONDS` - default `5`

Example:

```bash
WATCHDOG_HEALTH_URL="https://example.com/loader/healthz" \
WATCHDOG_RESTART_COMMAND="systemctl restart open-workshop-loader" \
python watchdog.py
```

### 6. Open the API docs

- Distributor Swagger UI: `https://example.com/distributor/`
- Distributor OpenAPI JSON: `https://example.com/distributor/openapi.json`
- Loader Swagger UI: `https://example.com/loader/`
- Loader OpenAPI JSON: `https://example.com/loader/openapi.json`

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [API Reference](docs/API.md)
- [Configuration](docs/CONFIGURATION.md)
- [Development](docs/DEVELOPMENT.md)

## API At A Glance

The paths below stay at their natural root locations. On a shared domain, keep only the docs and health
endpoints behind service-specific prefixes, and leave the functional routes unchanged.

| Service | Method | Path | Purpose |
| --- | --- | --- | --- |
| Distributor | `GET` / `HEAD` | `/download/{type}/{path:path}` | Download stored files, with access-service validation for protected mod archives |
| Distributor | `POST` | `/blurhashes` | Generate BlurHash metadata for stored images |
| Loader | `POST` | `/upload` | Internal multipart upload endpoint for Manager |
| Loader | `DELETE` | `/delete` | Internal delete endpoint for Manager |
| Loader | `GET` / `POST` | `/transfer/start` | Start background download and repack flow from transfer JWT |
| Loader | `POST` | `/transfer/upload` | Upload archive or image as raw body using transfer JWT |
| Loader | `WS` | `/transfer/ws/{job_id}` | Subscribe to live transfer progress |
| Loader | `POST` | `/transfer/repack` | Repack an already uploaded source file |
| Loader | `POST` | `/transfer/move` | Move packed file to permanent storage |

Detailed request and response semantics: [docs/API.md](docs/API.md)

## Runtime Model

The loader service keeps active job state in Redis and uses the local process only as a cache for active
connections and in-flight work. Per-job files still live under `<MAIN_DIR>/temp/<job_id>/`.

That design keeps the loader code simple and fast, but it also means:

- Redis must be reachable for shared job state and websocket fan-out
- temp files still live on the shared storage path under `MAIN_DIR`
- local websocket connections are still tied to the process that accepted them

More details: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Project Layout

```text
src/open_workshop_storage/
├── api/routes/         # FastAPI endpoints
├── core/               # shared state contracts and metadata helpers
├── observability/      # OpenTelemetry / Uptrace wiring
├── services/           # long-running transfer workflows
├── distributor.py      # distributor app entrypoint
├── loader.py           # loader app entrypoint
├── service_factory.py  # shared app wiring and router cloning helpers
└── utils/              # archive, auth, file, and image utilities
```

## Quality Tooling

The repository ships with a small `Makefile` for formatting, linting, and type checking:

```bash
make format
make lint
make type-check
```

Toolchain:

- `black` for code style
- `isort` for imports
- `flake8` for linting
- `mypy` for static type checks

`make lint` verifies `isort`, `black`, and `flake8`, while `make format` applies `isort` and `black`.

Development workflow details: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)

## Telemetry

If `UPTRACE_DSN` is configured, the app enables OpenTelemetry tracing and exports spans to Uptrace.

Example:

```bash
export UPTRACE_DSN="https://<token>@api.uptrace.dev/<project_id>"
export OTEL_SERVICE_NAME="open-workshop-storage"
export OTEL_SERVICE_VERSION="1.0.0"
export OTEL_DEPLOYMENT_ENVIRONMENT="production"
granian --working-dir src --interface asgi --host 127.0.0.1 --port 7070 open_workshop_storage.app:app
```

Telemetry settings reference: [docs/CONFIGURATION.md](docs/CONFIGURATION.md)

## License

This project is distributed under the terms of the MPL-2.0 license. See [LICENSE](LICENSE).
