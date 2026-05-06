# Open Workshop Storage

Rust rewrite of the Open Workshop storage services, with the original split preserved:

- `distributor` serves stored files and BlurHash metadata
- `loader` handles uploads, transfer jobs, repacks, moves, and websocket progress

Both binaries share the same environment-variable configuration and the same storage layout on disk.

## Requirements

- Rust toolchain with `cargo`
- `7z` from `p7zip-full` or an equivalent package
- Environment variables documented in `docs/CONFIGURATION.md`

Ubuntu / Debian:

```bash
sudo apt update
sudo apt install -y cargo rustc p7zip-full
```

## Setup

Set at least:

- `MAIN_DIR`
- `MANAGER_URL`
- `ACCESS_SERVICE_URL`
- `TRANSFER_JWT_SECRET`
- token hashes for `DELETE_FILE`, `UPLOAD_FILE`, and `STORAGE_MANAGE_TOKEN`

Generate token pairs with:

```bash
cargo run --bin token_gen
```

## Build

```bash
cargo check
cargo test
cargo build --release
```

## Run

Distributor:

```bash
OPEN_WORKSHOP_PORT=8000 cargo run --bin distributor
```

Loader:

```bash
OPEN_WORKSHOP_PORT=8001 cargo run --bin loader
```

The services also respect `OPEN_WORKSHOP_HOST`, which defaults to `0.0.0.0`.

For systemd deployment, see [docs/DEPLOYMENT.md](/home/admin1/Documents/GitHub/open-workshop-storage/docs/DEPLOYMENT.md).

## Convenience Commands

```bash
make check
make test
make fmt
make lint
make run-distributor
make run-loader
```

## API

Main routes:

- Distributor: `GET /`, `GET /redoc/`, `GET /healthz`, `GET /openapi.json`, `POST /blurhashes`, `GET|HEAD /download/:storage_type/*path`
- Loader: `GET /`, `GET /redoc/`, `GET /healthz`, `GET /openapi.json`, `POST /upload`, `DELETE /delete`, `GET|POST /transfer/start`, `POST /transfer/upload`, `WS /transfer/ws/:job_id`, `POST /transfer/repack`, `POST /transfer/move`

## Notes

- Archive handling still uses `7z`
- Image uploads are normalized to WebP
- Transfer downloads still validate access through the access service
- Configuration is provided through environment variables; see `docs/CONFIGURATION.md`
