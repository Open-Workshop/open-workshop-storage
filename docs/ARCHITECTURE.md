# Architecture

## Overview

Open Workshop Storage is implemented as two FastAPI entrypoints that share the same storage root and helper code.
They expose different external responsibilities:

- `distributor` serves permanent files and blurhash metadata
- `loader` executes transfer jobs, uploads, repacks, and storage maintenance actions

The loader side now keeps active transfer state, job metadata, and websocket fan-out behind Redis when
`REDIS_URL` is configured. That lets the loader run with multiple workers while keeping a local cache only
for active websocket clients and in-flight work. If Redis is omitted, the code falls back to an in-memory
store for development and tests.

The distributor side keeps a local BlurHash LRU cache for fast repeat hits inside one process, and when
Redis is configured it also writes shared BlurHash results into Redis so other workers can reuse them.
That keeps the distributor fast while avoiding duplicate image hashing across processes.

When both services are mounted on a single domain, use distinct path prefixes such as `/distributor` and
`/loader` for the conflicting docs and health endpoints. The functional routes below remain root-relative,
for example `/download/...`, `/upload`, and `/transfer/...`.

## Why Redis

Redis is the shared source of truth for active transfer jobs when configured.

That shared state contains:

- current status and stage
- byte counters
- optional archive progress percent
- metadata used by transfer maintenance endpoints
- active job identifiers for cleanup

The local process still keeps websocket connections and a small cache in memory, but that cache is now
replicated from Redis events rather than being the authority.

BlurHash cache entries are also stored in Redis when available, but they remain an optimization rather
than the source of truth because the local in-process LRU cache still serves same-worker repeat requests.

## Main Components

### `src/open_workshop_storage/app.py`

Legacy combined bootstrap and runtime wiring kept for compatibility:

- creates the FastAPI app
- enables telemetry
- owns the local job caches and Redis-backed storage helpers
- starts the cleanup loop and Redis event listener in lifespan
- provides the shared `ServiceContext`

### `src/open_workshop_storage/loader.py`

Loader service entrypoint:

- exposes `/upload`, `/delete`, and all `/transfer/*` endpoints
- owns transfer job progress, callbacks, and cleanup lifecycle
- can run with multiple workers when Redis is configured

### `src/open_workshop_storage/distributor.py`

Distributor service entrypoint:

- exposes `/download/{type}/{path:path}` and `/blurhashes`
- validates protected archive downloads through the access service
- can be scaled independently from the loader side

### `src/open_workshop_storage/service_factory.py`

Shared app wiring:

- installs request-scoped service context
- clones routers into service-specific entrypoints
- applies common CORS, telemetry, and health checks

### `src/open_workshop_storage/api/routes/`

HTTP and WebSocket layer:

- `files.py` contains the shared file endpoints used by both service entrypoints
- `transfers.py` contains the loader-only transfer orchestration endpoints and progress WebSocket

### `src/open_workshop_storage/services/transfer_jobs.py`

Long-running workflows:

- background URL download jobs
- archive extraction and repacking
- callback delivery to Manager
- inactivity cleanup

### `src/open_workshop_storage/core/`

Shared contracts and metadata utilities:

- service context definition
- job-state helpers
- metadata patch/update helpers

### `src/open_workshop_storage/utils/`

Low-level helpers:

- JWT and token helpers
- archive inspection and `7z` integration
- path safety
- image conversion to WebP

## Filesystem Layout

Inside `MAIN_DIR` the service uses a predictable directory layout:

```text
<MAIN_DIR>/
├── archive/            # permanent archives
├── avatar/             # permanent avatar images
├── resource/           # permanent generic resources
└── temp/
    └── <job_id>/       # transient transfer workspace
        ├── source.zip / upload.img / ...
        ├── repack/
        └── packed.zip / packed.webp
```

### Important notes

- `safe_path()` is used whenever user-controlled paths are resolved.
- `avatar` uploads are restricted to image content and are stored as `.webp`.
- transfer workspaces are cleaned by TTL and also removed after move / failure cleanup.

## Transfer Flows

### URL-Based Transfer Flow

Triggered by `GET` or `POST /transfer/start` on the loader service.

1. A client sends a transfer JWT with audience `storage`.
2. The service validates `job_id`, `download_url`, and packing options.
3. Job metadata is written to Redis under job-specific keys.
4. A background task downloads the source file.
5. The service emits progress updates through WebSocket.
6. The downloaded archive is inspected with `7z`.
7. If needed, the source is extracted and repacked to `packed.zip`.
8. A signed callback is sent to Manager with success or failure status.

### Raw Upload Transfer Flow

Triggered by `POST /transfer/upload` on the loader service.

1. A client sends raw binary request body and a transfer JWT.
2. The JWT describes whether the upload is an `archive` or `img` transfer.
3. The service streams the body to disk while updating progress.
4. Archive uploads are validated and repacked to ZIP.
5. Image uploads are converted to WebP.
6. Completion is broadcast to WebSocket subscribers.
7. A signed callback is sent to Manager.

### Maintenance Flow

Manager can also operate on prepared jobs via the loader service:

- `POST /transfer/repack`
- `POST /transfer/move`

These endpoints require `storage_manage_token`.

## Job Stages

The job state model is stage-based. Common stages include:

- `pending`
- `uploading`
- `uploaded`
- `downloading`
- `downloaded`
- `extracting`
- `repacking`
- `processing`
- `packed`
- `error`

For archive jobs, `percent` is meaningful during `extracting` and `repacking`.

## Metadata Model

Each job has a Redis metadata record. Depending on the flow, it may contain:

- source file information
- packing configuration
- download or upload byte counters
- packed artifact path and size
- final moved path
- timestamps for creation, completion, and move
- error state

This metadata is useful for maintenance endpoints and debugging, and Redis makes it safe to read from any
worker process.

## Manager Integration

The service communicates with Manager in two directions:

### Access checks for protected downloads

When downloading `archive/mods/...`, the service queries Manager to confirm the current user can access the
mod archive.

### Completion callbacks

After transfer success or failure, the service sends a JWT-signed callback to Manager. The callback target is:

- `MANAGER_TRANSFER_CALLBACK_URL` if configured
- otherwise `<MANAGER_URL>/internal/storage/transfer-completions`

## Cleanup

The cleanup loop runs every `CLEANUP_INTERVAL_SECONDS` seconds.

It removes:

- completed jobs from Redis and the local cache immediately after `POST /transfer/move` finalizes the transfer
- inactive jobs from Redis and the local cache when their `last_activity` exceeds `JOB_TTL_SECONDS`
- stale temp directories on disk that no longer have active state in Redis or the local cache

## Security Model

The service uses two authentication patterns:

- bcrypt-verified form tokens for internal file-management endpoints
- JWTs signed with `TRANSFER_JWT_SECRET` for transfer flows and callbacks

Additional protections:

- path traversal protection through `safe_path()`
- upload-type and file-kind allowlists
- JWT `aud` validation
- encrypted ZIP rejection

## Operational Caveats

- `7z` must be available in `PATH`.
- the loader service can be deployed with multiple workers when Redis is configured
- CORS is permissive in the current middleware (`*`)
- websocket progress depends on the Redis event listener staying healthy
