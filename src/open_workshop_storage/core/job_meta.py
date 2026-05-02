from __future__ import annotations

from typing import Any

from .context import ServiceContext


async def update_job_meta(
    ctx: ServiceContext,
    job_id: str,
    updates: dict[str, Any],
    *,
    warning_message: str = "failed to update meta for job_id=%s",
) -> None:
    try:
        meta = await ctx.read_meta(job_id)
        if meta is None:
            ctx.logger.warning(warning_message, job_id)
            return
        meta.update(updates)
        await ctx.write_meta(job_id, meta)
    except Exception:
        ctx.logger.warning(warning_message, job_id)
