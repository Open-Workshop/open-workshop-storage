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
uvicorn main:app --host 127.0.0.1 --port 7070
```

Опционально можно переопределить OTLP endpoint:

```bash
export UPTRACE_OTLP_TRACES_URL="https://api.uptrace.dev/v1/traces"
# export UPTRACE_OTLP_GRPC_URL="https://api.uptrace.dev:4317"
```
