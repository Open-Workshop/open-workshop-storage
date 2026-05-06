# API Reference

This document is a route-level reference for the Rust `distributor` and `loader` binaries.
The implementation also exposes Swagger UI in the root of each service, ReDoc at `/redoc/`, and OpenAPI JSON alongside it.
When both services share one domain, only the health and docs routes are typically namespaced; the
functional routes below stay at their natural root paths.

## Service Split

- Distributor service: `/`, `/redoc/`, `/healthz`, `/openapi.json`, `/download/{type}/{path:path}`, `/blurhashes`
- Loader service: `/`, `/redoc/`, `/healthz`, `/openapi.json`, `/upload`, `/delete`, and all `/transfer/*` endpoints

## Authentication Summary

The service uses two authentication mechanisms.

### Internal token-based endpoints

These endpoints expect a plain token from the caller and validate it against configured secrets:

- `/upload` uses `upload_file`
- `/delete` uses `delete_file`
- `/transfer/repack` uses `storage_manage_token`
- `/transfer/move` uses `storage_manage_token`

### Access-service endpoints

Protected archive downloads call the access service, which reads the request cookies and resolves mod rights:

- `/download/{type}/{path:path}` for `archive/mods/...`
- `/download/{type}/{path:path}` for `resource/mods/...` and `resource/modpacks/...`

### Transfer JWT endpoints

These endpoints expect a JWT signed with `TRANSFER_JWT_SECRET` and audience `storage`:

- `/transfer/start`
- `/transfer/upload`
- `/transfer/ws/{job_id}`

Manager callbacks are signed by the storage service with audience `manager`.

## File Endpoints

### `GET` / `HEAD` `/download/{type}/{path:path}`

Download a stored file.

### Path parameters

- `type`: one of `archive`, `resource`, `avatar`
- `path`: relative path inside the selected storage root

### Query parameters

- `filename`: optional safe filename prefix for the outgoing download name

### Special behavior

- for `archive/mods/...` downloads, access is validated through the access service
- when the access service denies a download, its human-readable reason is returned to the client
- `HEAD` requests for mod archives may include `X-Unpacked-Bytes`

### Common responses

- `200` file streamed successfully
- `400` invalid storage type
- `403` access denied by Manager
- `403` access denied by the access service, with its reason text returned to the client
- `404` file not found
- `423` path traversal blocked
- `503` Access service unavailable

### `POST` `/upload`

Internal multipart upload endpoint used by Manager.

### Form fields

- `file`: uploaded file
- `type`: `resource` or `avatar`
- `path`: relative destination path
- `file_kind`: `img` or `bin`
- `token`: plain upload token

### Rules

- `avatar` requires image file kind
- image uploads are converted to WebP
- avatar storage path must end with `.webp`

### Common responses

- `201` uploaded successfully, response body contains saved relative path
- `400` invalid type or file kind
- `401` token missing
- `403` invalid token
- `423` path traversal blocked

### `DELETE` `/delete`

Internal delete endpoint used by Manager.

### Form fields

- `type`: `archive`, `resource`, or `avatar`
- `path`: relative path to remove
- `token`: plain delete token

### Behavior

- deletes the file
- removes now-empty parent directories inside the storage root

### Common responses

- `200` file deleted
- `400` invalid type
- `401` token missing
- `403` invalid token or blocked path
- `404` file not found

### `POST` `/blurhashes`

Generates BlurHash strings for a batch of stored image URLs or storage-relative download paths.

### Request body

- `paths`: list of storage download URLs or relative paths like `download/resource/mods/...` or `download/resource/modpacks/...`

### Response body

- `items`: list of results in the same order as the request
- each item contains `path`, `blurhash`, `width`, and `height`

### Behavior

- each item is processed independently
- invalid paths and non-image files return `null` fields for that item instead of failing the whole batch
- response preserves request order
- repeated identical file targets inside one batch are computed once and reused
- computed blurhash values are kept in an in-memory LRU cache, default size `4096`; larger values increase per-worker RSS
- when `REDIS_URL` is configured, computed blurhash values are also shared through Redis with a TTL-based cache entry, so other workers can reuse them without recomputing

### Common responses

- `200` BlurHash batch generated successfully
- `400` invalid request body

## Transfer Endpoints

### `GET` / `POST` `/transfer/start`

Start a background transfer from a download URL embedded in a transfer JWT.

### Token sources

- query parameter `token`
- form field `token` for `POST`

### Expected JWT payload

- `job_id`: required
- `download_url`: required
- `filename`: optional
- `mod_id`: optional
- `pack_format`: optional, currently only `zip`
- `pack_level`: optional, defaults to `3`
- `max_bytes`: optional
- `mode`: optional archive mode from Manager, typically `create` or `replace`
- `update_only` or `keep_condition`: optional manager callback hint

### Response

Returns:

- `job_id`
- `status`
- `ws_url`

If a job is already started in memory, the current job status is returned instead of starting a duplicate.

### `POST` `/transfer/upload`

Upload a transfer body directly as raw binary.

### Auth

- query `token`
- or `Authorization: Bearer <transfer_jwt>`

### Optional filename sources

- query `filename`
- header `X-File-Name`

### Expected JWT payload

Common fields:

- `job_id`
- `transfer_kind`: `archive` or `img`
- `callback_action`
- `callback_context`
- `target_path`
- `max_bytes`

Archive-specific fields:

- `mod_id`
- `pack_format`
- `pack_level`
- `mode`
- `condition` is derived for archive jobs:
  - `create` -> `draft`
  - `replace` -> `published`
- `update_only` or `keep_condition`

Image-specific fields:

- `storage_type`
- `file_kind` (must resolve to `img`)

### Behavior

- raw body is streamed to disk
- progress is emitted to `/transfer/ws/{job_id}`
- archive uploads are inspected and repacked to ZIP
- image uploads are converted to WebP
- Manager receives success or error callback

### Common responses

- `200` transfer accepted and packed
- `400` invalid job or unsupported payload
- `401` token missing
- `403` invalid JWT
- `408` upload timed out
- `413` size limit exceeded
- `429` storage is busy processing another transfer
- `500` packing or upload failure

### `WS` `/transfer/ws/{job_id}`

Subscribe to live job progress.

### Auth

- query `token`

The JWT must be valid for audience `storage` and must contain the same `job_id` as the WebSocket path.

### Event types

- `progress`
- `stage`
- `complete`
- `error`

### Example snapshot

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

### Notes

- clients receive a current snapshot immediately after connect
- `percent` is primarily used during archive extraction and repack
- clients can reconnect and replace local state from the latest snapshot

### `POST` `/transfer/repack`

Repack an existing uploaded source file for a job.

### Form fields

- `job_id`
- `format` currently only `zip`
- `compression_level`
- `token`

### Responses

- `200` packed file info
- `400` invalid job or unsupported format
- `401` token missing
- `403` invalid token
- `404` job or source file not found
- `413` unpacked archive size limit exceeded
- `429` storage is busy processing another repack
- `500` repack failed

### `POST` `/transfer/move`

Move the packed artifact from temp storage to permanent storage.

### Form fields

- `job_id`
- `type`
- `path`
- `token`

### Behavior

- moves the packed file to the target storage root
- records final path and size in metadata
- clears the Redis-backed job state and closes any remaining websocket subscribers
- removes the temp job directory afterwards

### Responses

- `200` final path and byte size
- `400` invalid type or job id
- `401` token missing
- `403` invalid token
- `404` job or packed file not found
- `423` path traversal blocked

## Manager Callback

After transfer completion, the service sends a callback to Manager with a JWT in the `Authorization` header.
If `MANAGER_TRANSFER_CALLBACK_URL` is not configured, the fallback target is
`<MANAGER_URL>/internal/storage/transfer-completions`.

Possible payload fields include:

- `job_id`
- `status`
- `reason`
- `bytes`
- `total`
- `unpacked_bytes`
- `packed_format`
- `mod_id`
- `storage_type`
- `file_kind`
- `callback_action`
- `callback_context`
- `target_path`
- `mode` for archive jobs
- `condition` for archive jobs, derived from the archive mode

The exact fields depend on the transfer flow and whether the job succeeded or failed.
