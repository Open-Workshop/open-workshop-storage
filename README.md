# Open Workshop Storage

## Requirements
- Python 3.10+
- System dependency: 7z (p7zip-full)

### Ubuntu / Debian
```bash
sudo apt update
sudo apt install -y p7zip-full
```

## Install Python deps
```bash
pip install -r requirements.txt
```

## Run Server

Production-like start:

```bash
granian --working-dir src --interface asgi --host 0.0.0.0 --port 8000 --access-log open_workshop_storage.app:app
```

Local start:

```bash
granian --working-dir src --interface asgi --host 127.0.0.1 --port 8000 open_workshop_storage.app:app
```

The service is expected to run as a single worker process.

## Project Layout

Application code lives under `src/open_workshop_storage/`.

- `api/routes/` contains FastAPI endpoints.
- `services/` contains long-running job workflows.
- `core/` contains shared application state and helpers.
- `utils/` contains archive, auth, file, and image utilities.
- `observability/` contains telemetry wiring.

## Transfer WebSocket

Progress for `/transfer/start` and `/transfer/upload` jobs is available via
`/transfer/ws/{job_id}?token=...`.

When a client connects, the server immediately sends the current snapshot:

```json
{
  "event": "progress",
  "bytes": 1048576,
  "total": 7340032,
  "status": "uploading",
  "stage": "uploading",
  "percent": null
}
```

Supported WebSocket events:

- `progress`:
  - During `uploading` and `downloading`, sent at most once every `0.25s` while bytes are still flowing.
  - During `extracting` and `repacking`, includes optional `percent` from `7z` so the client can render archive progress for each phase.
- `stage`: sent whenever the transfer stage changes, for example `uploading`, `uploaded`, `downloading`, `downloaded`, `extracting`, `repacking`, `packed`.
- `complete`: sent when the final packed artifact is ready.
- `error`: sent when the transfer or repack fails.

Example progress event during archive repacking:

```json
{
  "event": "progress",
  "bytes": 7340032,
  "total": 7340032,
  "status": "done",
  "stage": "repacking",
  "percent": 42
}
```

Notes:

- Every WebSocket event includes the current state snapshot (`bytes`, `total`, `status`, `stage`), so clients can safely replace local state even if they connected while a job was still `pending`.
- `percent` is only meaningful for `stage = "extracting"` and `stage = "repacking"`. For other stages it is `null` in the initial snapshot and may be omitted in subsequent events.
- There is no heartbeat timer. Progress messages are emitted on actual state changes or data/progress updates.

## Uptrace telemetry

Сервер отправляет трейсы в Uptrace через OpenTelemetry, если задан `UPTRACE_DSN`.

Пример запуска:

```bash
export UPTRACE_DSN="https://<token>@api.uptrace.dev/<project_id>"
export OTEL_SERVICE_NAME="open-workshop-storage"
export OTEL_SERVICE_VERSION="1.0.0"
export OTEL_DEPLOYMENT_ENVIRONMENT="production"
# export UPTRACE_OTLP_PROTOCOL="grpc"   # or "http"
# export UPTRACE_FASTAPI_EXCLUDED_URLS="^.*/docs$,^.*/openapi\\.json$,^/favicon\\.ico$,^/robots\\.txt$"
# export UPTRACE_FASTAPI_EXCLUDE_SPANS="receive,send"
granian --working-dir src --interface asgi --host 127.0.0.1 --port 7070 open_workshop_storage.app:app
```

Опционально можно переопределить OTLP endpoint:

```bash
export UPTRACE_OTLP_TRACES_URL="https://api.uptrace.dev/v1/traces"
# export UPTRACE_OTLP_GRPC_URL="https://api.uptrace.dev:4317"
```
