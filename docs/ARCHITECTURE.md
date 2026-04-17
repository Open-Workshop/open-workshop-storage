# Architecture

## Overview

Open Workshop Storage is a FastAPI service that acts as the storage and transfer backend for Open Workshop.
It has two main responsibilities:

- serve and manage files in permanent storage
- execute transfer jobs that download, upload, repack, and move artifacts

The current design is intentionally single-process and single-worker. That is not an incidental deployment
detail; it is part of the architecture.

## Why Single Worker

Active transfer state is stored in the in-memory `JOB_STATE` dictionary inside
`src/open_workshop_storage/app.py`.

That state contains:

- current status and stage
- byte counters
- optional archive progress percent
- active WebSocket connections for the job
- last activity timestamps used by cleanup

Because this state is process-local:

- a second worker would not see jobs started by the first worker
- WebSocket progress would become inconsistent across workers
- cleanup and callbacks could race or miss active jobs

If multi-worker or horizontal scaling is ever required, the project will first need shared state for jobs,
events, and socket fan-out.

## Main Components

### `src/open_workshop_storage/app.py`

Application bootstrap and runtime wiring:

- creates the FastAPI app
- enables telemetry
- owns `JOB_STATE` and `JOB_LOCK`
- starts the cleanup loop in lifespan
- provides the shared `ServiceContext`

### `src/open_workshop_storage/api/routes/`

HTTP and WebSocket layer:

- `files.py` handles stored file download, internal upload, and delete
- `transfers.py` handles transfer orchestration endpoints and progress WebSocket

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
        ├── meta.json
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

Triggered by `GET` or `POST /transfer/start`.

1. A client sends a transfer JWT with audience `storage`.
2. The service validates `job_id`, `download_url`, and packing options.
3. Job metadata is created under `temp/<job_id>/meta.json`.
4. A background task downloads the source file.
5. The service emits progress updates through WebSocket.
6. The downloaded archive is inspected with `7z`.
7. If needed, the source is extracted and repacked to `packed.zip`.
8. A signed callback is sent to Manager with success or failure status.

### Raw Upload Transfer Flow

Triggered by `POST /transfer/upload`.

1. A client sends raw binary request body and a transfer JWT.
2. The JWT describes whether the upload is an `archive` or `img` transfer.
3. The service streams the body to disk while updating progress.
4. Archive uploads are validated and repacked to ZIP.
5. Image uploads are converted to WebP.
6. Completion is broadcast to WebSocket subscribers.
7. A signed callback is sent to Manager.

### Maintenance Flow

Manager can also operate on prepared jobs via:

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

Each job has a `meta.json` file in its temp directory. Depending on the flow, it may contain:

- source file information
- packing configuration
- download or upload byte counters
- packed artifact path and size
- final moved path
- timestamps for creation, completion, and move
- error state

This metadata is useful for maintenance endpoints and debugging, but it is not a replacement for distributed
job storage.

## Manager Integration

The service communicates with Manager in two directions:

### Access checks for protected downloads

When downloading `archive/mods/...`, the service queries Manager to confirm the current user can access the
mod archive.

### Completion callbacks

After transfer success or failure, the service sends a JWT-signed callback to Manager. The callback target is:

- `MANAGER_TRANSFER_CALLBACK_URL` if configured
- otherwise `<MANAGER_URL>/storage/transfer/complete`

## Cleanup

The cleanup loop runs every `CLEANUP_INTERVAL_SECONDS` seconds.

It removes:

- inactive jobs from memory when their `last_activity` exceeds `JOB_TTL_SECONDS`
- stale temp directories on disk that no longer have active in-memory state

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
- this service should be deployed as a single worker
- CORS is permissive in the current middleware (`*`)
- WebSocket progress depends on the job process staying alive
