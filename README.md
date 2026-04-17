# Open Workshop Storage

[![imports-isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![BlackCode](https://img.shields.io/badge/code%20style-black-black)](https://github.com/psf/black)
[![mypy](https://img.shields.io/badge/type--checked-mypy-blue?logo=python)](https://mypy.readthedocs.io/en/stable/index.html)
[![Discord](https://img.shields.io/discord/792572437292253224?label=Discord&labelColor=%232c2f33&color=%237289da)](https://discord.gg/UnJnGHNbBp)
[![Telegram](https://img.shields.io/badge/Telegram-24A1DE)](https://t.me/miskler_dev)

Single-worker storage backend for Open Workshop.

The service stores uploaded files, validates download access for protected archives, ingests transfer jobs
from external URLs or raw uploads, repacks archives to ZIP, converts images to WebP, streams progress over
WebSocket, and reports completion back to Manager.

## Highlights

- Single-worker runtime designed around in-memory job state.
- FastAPI application served with Granian.
- Protected archive downloads with Manager-side access validation.
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
- `TRANSFER_JWT_SECRET`
- token values in `ow_config.py`

Configuration details: [docs/CONFIGURATION.md](docs/CONFIGURATION.md)

### 4. Generate tokens

```bash
./.venv/bin/python token_gen.py
```

### 5. Start the service

Local:

```bash
granian --working-dir src --interface asgi --host 127.0.0.1 --port 8000 open_workshop_storage.app:app
```

Server:

```bash
granian --working-dir src --interface asgi --host 0.0.0.0 --port 8000 --access-log open_workshop_storage.app:app
```

The service is expected to run as a single worker process. Multi-worker deployment is not supported by the
current architecture because active transfer state lives in process memory.

### 6. Open the API docs

- Swagger UI: `http://127.0.0.1:8000/`
- OpenAPI JSON: `http://127.0.0.1:8000/openapi.json`

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [API Reference](docs/API.md)
- [Configuration](docs/CONFIGURATION.md)
- [Development](docs/DEVELOPMENT.md)

## API At A Glance

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` / `HEAD` | `/download/{type}/{path:path}` | Download stored files, with Manager validation for protected mod archives |
| `POST` | `/upload` | Internal multipart upload endpoint for Manager |
| `DELETE` | `/delete` | Internal delete endpoint for Manager |
| `GET` / `POST` | `/transfer/start` | Start background download and repack flow from transfer JWT |
| `POST` | `/transfer/upload` | Upload archive or image as raw body using transfer JWT |
| `WS` | `/transfer/ws/{job_id}` | Subscribe to live transfer progress |
| `POST` | `/transfer/repack` | Repack an already uploaded source file |
| `POST` | `/transfer/move` | Move packed file to permanent storage |

Detailed request and response semantics: [docs/API.md](docs/API.md)

## Runtime Model

Open Workshop Storage keeps active job state in memory and persists per-job metadata under
`<MAIN_DIR>/temp/<job_id>/meta.json`.

That design keeps the code simple and fast for a single service instance, but it also means:

- one process must own the whole lifecycle of a transfer job
- WebSocket clients for a job must connect to the same process that started it
- horizontal fan-out or multi-worker ASGI deployment needs a shared state layer before it becomes safe

More details: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Project Layout

```text
src/open_workshop_storage/
├── api/routes/         # FastAPI endpoints
├── core/               # shared state contracts and metadata helpers
├── observability/      # OpenTelemetry / Uptrace wiring
├── services/           # long-running transfer workflows
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
