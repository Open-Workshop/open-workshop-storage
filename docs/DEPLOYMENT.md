# Deployment

The Rust version no longer uses `granian`, Python entrypoints, or `ow_config.py`.
Configure both services through environment variables and run the compiled Rust binaries directly.

## Shared Environment File

Create a root-owned env file, for example:

```bash
sudo install -d -m 0750 /etc/open-workshop-storage
sudo nano /etc/open-workshop-storage/open-workshop-storage.env
```

Example contents:

```bash
MAIN_DIR=/var/www/open-workshop-storage/storage
ACCESS_SERVICE_URL=https://access.openworkshop.miskler.ru
ACCESS_SERVICE_TIMEOUT_SECONDS=5
MANAGER_URL=https://api.openworkshop.miskler.ru
MANAGER_TRANSFER_CALLBACK_URL=
TRANSFER_JWT_SECRET=replace-me
TRANSFER_CALLBACK_TTL_SECONDS=600
TRANSFER_MAX_BYTES=0

DELETE_FILE=replace-with-bcrypt-hash
UPLOAD_FILE=replace-with-bcrypt-hash
STORAGE_MANAGE_TOKEN=replace-with-bcrypt-hash

RUST_LOG=info

# Optional:
# REDIS_URL=redis://127.0.0.1:6379/0
# REDIS_PREFIX=open-workshop-storage
```

`DELETE_FILE`, `UPLOAD_FILE`, and `STORAGE_MANAGE_TOKEN` must contain bcrypt hashes.
Generate matching plain values and hashes with:

```bash
cargo run --bin token_gen
```

## systemd: Loader

```ini
[Unit]
Description=Open Workshop Storage Loader
After=network.target redis-server.service

[Service]
User=www-data
Group=www-data
WorkingDirectory=/var/www/open-workshop-storage
EnvironmentFile=/etc/open-workshop-storage/open-workshop-storage.env
Environment=OPEN_WORKSHOP_HOST=127.0.0.1
Environment=OPEN_WORKSHOP_PORT=8001
ExecStart=/var/www/open-workshop-storage/target/release/loader
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## systemd: Distributor

```ini
[Unit]
Description=Open Workshop Storage Distributor
After=network.target redis-server.service

[Service]
User=www-data
Group=www-data
WorkingDirectory=/var/www/open-workshop-storage
EnvironmentFile=/etc/open-workshop-storage/open-workshop-storage.env
Environment=OPEN_WORKSHOP_HOST=127.0.0.1
Environment=OPEN_WORKSHOP_PORT=8000
ExecStart=/var/www/open-workshop-storage/target/release/distributor
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Build and Reload

```bash
cargo build --release
sudo systemctl daemon-reload
sudo systemctl restart open-workshop-storage-loader open-workshop-storage-distributor
```

## Notes

- `check_access` from the old Python config is not used by the Rust storage service.
- `UPTRACE_*` and `OTEL_*` variables from the old setup are not wired up in the current Rust code.
- If you want to use Redis-backed shared state, set `REDIS_URL`; otherwise the service falls back to in-memory state.
