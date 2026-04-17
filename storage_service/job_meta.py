from __future__ import annotations

from typing import Any

import anyio

from .context import ServiceContext


async def update_job_meta(
    ctx: ServiceContext,
    job_id: str,
    updates: dict[str, Any],
    *,
    warning_message: str = "failed to update meta for job_id=%s",
) -> None:
    try:
        meta = await anyio.to_thread.run_sync(ctx.read_meta_sync, job_id)
        meta.update(updates)
        await anyio.to_thread.run_sync(ctx.write_meta_sync, job_id, meta)
    except Exception:
        ctx.logger.warning(warning_message, job_id)
