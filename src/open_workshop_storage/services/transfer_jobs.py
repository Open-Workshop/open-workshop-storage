from __future__ import annotations

import asyncio
import os
import shutil
import time
from typing import Optional

import aiohttp
import anyio

from ..core.context import JsonDict, ServiceContext
from ..core.job_meta import update_job_meta
from ..core.job_state import state_event_payload


def _read_positive_int_setting(ctx: ServiceContext, name: str, default: int = 0) -> Optional[int]:
    raw_value = getattr(ctx.config, name, default)
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        value = int(default)
    if value <= 0:
        return None
    return value


def _read_timeout_seconds(ctx: ServiceContext, name: str, default: float) -> Optional[float]:
    raw_value = getattr(ctx.config, name, default)
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        value = float(default)
    if value <= 0:
        return None
    return value


def _client_timeout(ctx: ServiceContext, name: str, default: float) -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(total=_read_timeout_seconds(ctx, name, default))


async def run_cleanup(ctx: ServiceContext) -> None:
    now = time.time()
    cleanup_threshold = getattr(ctx.config, "JOB_TTL_SECONDS", 10800)

    active_job_ids = set(await ctx.list_job_ids())
    local_job_ids = set(ctx.job_state.keys())
    job_ids = active_job_ids | local_job_ids
    jobs_to_remove: list[str] = []
    for job_id in job_ids:
        state = await ctx.read_job_state(job_id)
        if state is None:
            continue
        last_activity = state.get("last_activity", 0)
        if now - last_activity >= cleanup_threshold:
            jobs_to_remove.append(job_id)

    for job_id in jobs_to_remove:
        await ctx.delete_job_and_dir(job_id)
        ctx.logger.info("cleanup removed inactive job job_id=%s", job_id)

    try:
        if os.path.exists(ctx.temp_dir):
            for job_folder in os.listdir(ctx.temp_dir):
                job_path = os.path.join(ctx.temp_dir, job_folder)
                if not os.path.isdir(job_path):
                    continue
                if job_folder not in job_ids:
                    try:
                        mod_time = os.path.getmtime(job_path)
                        if now - mod_time >= cleanup_threshold:
                            await anyio.to_thread.run_sync(shutil.rmtree, job_path)
                            ctx.logger.info("cleanup removed old dir job_id=%s", job_folder)
                    except Exception:
                        ctx.logger.warning("failed to check/cleanup old dir job_id=%s", job_folder)
    except Exception:
        ctx.logger.exception("cleanup failed to scan temp dir")


async def cleanup_loop(ctx: ServiceContext) -> None:
    cleanup_interval = getattr(ctx.config, "CLEANUP_INTERVAL_SECONDS", 60)
    while True:
        try:
            await asyncio.sleep(cleanup_interval)
            await run_cleanup(ctx)
        except Exception:
            ctx.logger.exception("cleanup loop failed")


async def _broadcast_progress(ctx: ServiceContext, job_id: str, stage: str, percent: int) -> None:
    percent = max(0, min(100, int(percent)))
    await ctx.set_state(job_id, stage=stage, percent=percent)
    async with ctx.job_lock:
        state = ctx.job_state.setdefault(job_id, ctx.new_job_state())
        snapshot = state_event_payload("progress", state)
    await ctx.broadcast(job_id, snapshot)


async def _broadcast_repack_progress(ctx: ServiceContext, job_id: str, percent: int) -> None:
    await _broadcast_progress(ctx, job_id, "repacking", percent)


async def _broadcast_extract_progress(ctx: ServiceContext, job_id: str, percent: int) -> None:
    await _broadcast_progress(ctx, job_id, "extracting", percent)


async def notify_manager(ctx: ServiceContext, payload: JsonDict) -> None:
    callback_url = getattr(ctx.config, "MANAGER_TRANSFER_CALLBACK_URL", None) or (
        f"{ctx.manager_url}/internal/storage/transfer-completions"
    )
    ttl_raw = getattr(ctx.config, "TRANSFER_CALLBACK_TTL_SECONDS", 600)
    try:
        ttl_seconds = int(ttl_raw)
    except (TypeError, ValueError):
        ttl_seconds = 600
    token = ctx.tools.encode_transfer_jwt(payload, audience="manager", ttl_seconds=ttl_seconds)
    if not token:
        ctx.logger.warning("transfer callback skipped (missing JWT secret)")
        return
    headers = {"Authorization": f"Bearer {token}"}
    async with aiohttp.ClientSession(
        timeout=_client_timeout(ctx, "TRANSFER_CALLBACK_TIMEOUT_SECONDS", 30),
    ) as session:
        try:
            ctx.logger.info(
                "transfer callback send url=%s job_id=%s status=%s",
                callback_url,
                payload.get("job_id"),
                payload.get("status"),
            )
            async with session.post(callback_url, headers=headers) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    ctx.logger.warning(
                        "transfer callback failed status=%s body=%s",
                        resp.status,
                        body,
                    )
                else:
                    ctx.logger.info(
                        "transfer callback ok status=%s job_id=%s",
                        resp.status,
                        payload.get("job_id"),
                    )
        except Exception:
            ctx.logger.exception("transfer callback error")


async def run_repack_job(
    ctx: ServiceContext,
    job_id: str,
    download_abs: str,
    pack_format: str,
    pack_level: int,
) -> tuple[bool, Optional[str], Optional[int], Optional[int], Optional[str]]:
    async with ctx.repack_limiter.acquire_nowait() as acquired:
        if not acquired:
            ctx.logger.warning("transfer repack rejected (busy) job_id=%s", job_id)
            await ctx.set_state(job_id, status="error", error="busy")
            await ctx.broadcast(
                job_id,
                await ctx.build_state_event(job_id, "error", message="storage busy"),
            )
            await update_job_meta(
                ctx,
                job_id,
                {"status": "error", "error_reason": "busy"},
            )
            return False, None, None, None, "busy"

        return await _run_repack_job_limited(ctx, job_id, download_abs, pack_format, pack_level)


async def _run_repack_job_limited(
    ctx: ServiceContext,
    job_id: str,
    download_abs: str,
    pack_format: str,
    pack_level: int,
) -> tuple[bool, Optional[str], Optional[int], Optional[int], Optional[str]]:
    if pack_format != "zip":
        await ctx.set_state(job_id, status="error", error="unsupported_format")
        await ctx.broadcast(
            job_id,
            await ctx.build_state_event(job_id, "error", message="unsupported format"),
        )
        return False, None, None, None, "unsupported_format"

    packed_rel = os.path.join("temp", job_id, "packed.zip")
    packed_abs = ctx.tools.safe_path(ctx.main_dir, packed_rel)

    try:
        archive_type, is_encrypted, archive_entries = await anyio.to_thread.run_sync(
            ctx.tools.probe_archive,
            download_abs,
        )
        unpacked_bytes = await anyio.to_thread.run_sync(ctx.tools.archive_entries_unpacked_bytes, archive_entries)
    except Exception as exc:
        ctx.logger.exception("transfer archive probe failed job_id=%s", job_id)
        await ctx.set_state(job_id, status="error", error=str(exc))
        await ctx.broadcast(
            job_id,
            await ctx.build_state_event(job_id, "error", message="archive probe failed"),
        )
        return False, None, None, None, "repack_failed"

    max_unpacked_bytes = _read_positive_int_setting(ctx, "TRANSFER_MAX_UNPACKED_BYTES", 0)
    if is_encrypted:
        await ctx.set_state(job_id, status="error", error="encrypted_zip")
        await ctx.broadcast(
            job_id,
            await ctx.build_state_event(job_id, "error", message="zip encrypted"),
        )
        await update_job_meta(
            ctx,
            job_id,
            {"status": "error", "error_reason": "encrypted_zip"},
        )
        ctx.logger.warning("transfer repack denied (encrypted zip) job_id=%s", job_id)
        return False, None, None, unpacked_bytes, "encrypted_zip"

    if max_unpacked_bytes is not None and unpacked_bytes is not None and unpacked_bytes > max_unpacked_bytes:
        await ctx.set_state(job_id, status="error", error="unpacked_size_limit")
        await ctx.broadcast(
            job_id,
            await ctx.build_state_event(job_id, "error", message="archive too large"),
        )
        await update_job_meta(
            ctx,
            job_id,
            {
                "status": "error",
                "error_reason": "unpacked_size_limit",
                "unpacked_bytes": unpacked_bytes,
            },
        )
        ctx.logger.warning(
            "transfer repack denied (unpacked size limit) job_id=%s unpacked_bytes=%s limit=%s",
            job_id,
            unpacked_bytes,
            max_unpacked_bytes,
        )
        return False, None, None, unpacked_bytes, "unpacked_size_limit"

    if archive_type == "zip":
        zip_ok = await anyio.to_thread.run_sync(
            ctx.tools.zip_uses_deflated_or_better,
            download_abs,
            archive_entries,
        )
        if zip_ok:
            try:
                packed_rel = os.path.relpath(download_abs, ctx.main_dir)
                packed_abs = download_abs
                packed_bytes = os.path.getsize(packed_abs)
                await update_job_meta(
                    ctx,
                    job_id,
                    {
                        "packed_path": packed_rel,
                        "packed_bytes": packed_bytes,
                        "pack_format": pack_format,
                        "pack_level": pack_level,
                        "status": "packed",
                    },
                )
                await ctx.set_stage(job_id, "packed")
                ctx.logger.info(
                    "transfer repack skipped (zip ok) job_id=%s packed_bytes=%s",
                    job_id,
                    packed_bytes,
                )
                return True, packed_rel, packed_bytes, unpacked_bytes, None
            except Exception:
                ctx.logger.warning("failed to update meta for job_id=%s", job_id)

    if os.path.exists(packed_abs):
        packed_bytes = os.path.getsize(packed_abs)
        await update_job_meta(
            ctx,
            job_id,
            {
                "packed_path": packed_rel,
                "packed_bytes": packed_bytes,
                "pack_format": pack_format,
                "pack_level": pack_level,
                "status": "packed",
            },
        )
        await ctx.set_stage(job_id, "packed")
        return True, packed_rel, packed_bytes, unpacked_bytes, None

    try:
        repack_rel = os.path.join("temp", job_id, "repack")
        repack_abs = ctx.tools.safe_path(ctx.main_dir, repack_rel)
        if os.path.exists(repack_abs):
            await anyio.to_thread.run_sync(shutil.rmtree, repack_abs)
        os.makedirs(repack_abs, exist_ok=True)

        start_ts = time.monotonic()
        extract_progress = {"last_push": time.monotonic(), "last_percent": -1}
        repack_progress = {"last_push": time.monotonic(), "last_percent": -1}

        def _emit_progress(
            progress_state: dict[str, float | int],
            stage: str,
            percent: int,
        ) -> None:
            percent = max(0, min(100, int(percent)))
            last_percent = int(progress_state["last_percent"])
            now = time.monotonic()
            if percent <= last_percent:
                return
            if percent < 100 and now - float(progress_state["last_push"]) < ctx.progress_push_interval:
                return
            progress_state["last_percent"] = percent
            progress_state["last_push"] = now
            try:
                if stage == "extracting":
                    anyio.from_thread.run(_broadcast_extract_progress, ctx, job_id, percent)
                else:
                    anyio.from_thread.run(_broadcast_repack_progress, ctx, job_id, percent)
            except Exception:
                pass

        if archive_type:
            await ctx.set_stage(job_id, "extracting")
            await _broadcast_extract_progress(ctx, job_id, 0)

            def _on_extract_progress(percent: int) -> None:
                _emit_progress(extract_progress, "extracting", percent)

            await anyio.to_thread.run_sync(
                ctx.tools.safe_extract_archive,
                download_abs,
                repack_abs,
                archive_entries,
                _on_extract_progress,
            )
        else:
            dest_name = os.path.basename(download_abs)
            dest_path = os.path.join(repack_abs, dest_name)
            await anyio.to_thread.run_sync(shutil.move, download_abs, dest_path)
            await update_job_meta(
                ctx,
                job_id,
                {"download_path": os.path.relpath(dest_path, ctx.main_dir)},
                warning_message="failed to update meta download_path for job_id=%s",
            )

        await ctx.set_stage(job_id, "repacking")
        await _broadcast_repack_progress(ctx, job_id, 0)

        def _on_repack_progress(percent: int) -> None:
            _emit_progress(repack_progress, "repacking", percent)

        await anyio.to_thread.run_sync(
            ctx.tools.zip_dir_with_level,
            repack_abs,
            packed_abs,
            pack_level,
            _on_repack_progress,
        )
        if int(repack_progress["last_percent"]) < 100:
            await _broadcast_repack_progress(ctx, job_id, 100)
        duration = time.monotonic() - start_ts
        packed_bytes = os.path.getsize(packed_abs)
        await update_job_meta(
            ctx,
            job_id,
            {
                "packed_path": packed_rel,
                "packed_bytes": packed_bytes,
                "pack_format": pack_format,
                "pack_level": pack_level,
                "status": "packed",
            },
        )
        await ctx.set_stage(job_id, "packed")
        ctx.logger.info(
            "transfer repack done job_id=%s packed_bytes=%s duration=%.2fs",
            job_id,
            packed_bytes,
            duration,
        )
        return True, packed_rel, packed_bytes, unpacked_bytes, None
    except Exception as exc:
        ctx.logger.exception("transfer repack failed job_id=%s", job_id)
        await ctx.set_state(job_id, status="error", error=str(exc))
        await ctx.broadcast(
            job_id,
            await ctx.build_state_event(job_id, "error", message="repack failed"),
        )
        return False, None, None, unpacked_bytes, "repack_failed"


async def run_download_job(
    ctx: ServiceContext,
    job_id: str,
    download_url: str,
    download_abs: str,
    max_bytes: Optional[int],
    callback_payload: JsonDict,
) -> None:
    async with ctx.download_limiter.acquire_nowait() as acquired:
        if not acquired:
            ctx.logger.warning("transfer download rejected (busy) job_id=%s", job_id)
            await ctx.set_state(job_id, status="error", error="busy")
            await ctx.broadcast(
                job_id,
                await ctx.build_state_event(job_id, "error", message="storage busy"),
            )
            await update_job_meta(
                ctx,
                job_id,
                {"status": "error", "error_reason": "busy"},
            )
            await ctx.notify_manager({**callback_payload, "job_id": job_id, "status": "error", "reason": "busy"})
            await ctx.job_error_cleanup(job_id, "busy")
            return

        await _run_download_job_limited(ctx, job_id, download_url, download_abs, max_bytes, callback_payload)


async def _run_download_job_limited(
    ctx: ServiceContext,
    job_id: str,
    download_url: str,
    download_abs: str,
    max_bytes: Optional[int],
    callback_payload: JsonDict,
) -> None:
    await ctx.set_state(job_id, status="downloading", error=None)
    await ctx.set_stage(job_id, "downloading")
    downloaded = 0
    total = None
    last_push = 0.0
    await update_job_meta(
        ctx,
        job_id,
        {"status": "downloading", "download_started_at": int(time.time())},
        warning_message="failed to update meta (start) for job_id=%s",
    )
    try:
        async with aiohttp.ClientSession(
            timeout=_client_timeout(ctx, "TRANSFER_DOWNLOAD_TIMEOUT_SECONDS", 3600),
        ) as session:
            async with session.get(download_url) as resp:
                if resp.status != 200:
                    await ctx.set_state(job_id, status="error", error=f"status:{resp.status}")
                    await ctx.broadcast(
                        job_id,
                        await ctx.build_state_event(
                            job_id,
                            "error",
                            message=f"download failed with status {resp.status}",
                        ),
                    )
                    await ctx.notify_manager(
                        {
                            **callback_payload,
                            "job_id": job_id,
                            "status": "error",
                            "reason": f"status:{resp.status}",
                        }
                    )
                    return

                total = resp.content_length
                await ctx.set_state(job_id, total=total)
                if max_bytes and total and total > max_bytes:
                    await ctx.set_state(job_id, status="error", error="size_limit")
                    await ctx.broadcast(
                        job_id,
                        await ctx.build_state_event(job_id, "error", message="file too large"),
                    )
                    try:
                        if os.path.exists(download_abs):
                            os.remove(download_abs)
                    except Exception:
                        ctx.logger.warning("failed to cleanup partial file job_id=%s", job_id)
                    await ctx.notify_manager(
                        {
                            **callback_payload,
                            "job_id": job_id,
                            "status": "error",
                            "reason": "size_limit",
                        }
                    )
                    return

                os.makedirs(os.path.dirname(download_abs), exist_ok=True)
                with open(download_abs, "wb") as out_file:
                    async for chunk in resp.content.iter_chunked(1024 * 256):
                        if not chunk:
                            continue
                        downloaded += len(chunk)
                        if max_bytes and downloaded > max_bytes:
                            await ctx.set_state(job_id, status="error", error="size_limit")
                            await ctx.broadcast(
                                job_id,
                                await ctx.build_state_event(
                                    job_id,
                                    "error",
                                    message="file too large",
                                ),
                            )
                            try:
                                if os.path.exists(download_abs):
                                    os.remove(download_abs)
                            except Exception:
                                ctx.logger.warning("failed to cleanup partial file job_id=%s", job_id)
                            await ctx.notify_manager(
                                {
                                    **callback_payload,
                                    "job_id": job_id,
                                    "status": "error",
                                    "reason": "size_limit",
                                }
                            )
                            return
                        await anyio.to_thread.run_sync(out_file.write, chunk)
                        now = time.monotonic()
                        if now - last_push >= ctx.progress_push_interval:
                            last_push = now
                            await ctx.set_state(job_id, bytes=downloaded)
                            await ctx.broadcast(
                                job_id,
                                await ctx.build_state_event(job_id, "progress"),
                            )

        await ctx.set_state(job_id, status="done", bytes=downloaded, total=total)
        await update_job_meta(
            ctx,
            job_id,
            {
                "status": "downloaded",
                "downloaded_bytes": downloaded,
                "total_bytes": total,
                "download_completed_at": int(time.time()),
            },
        )
        await ctx.set_stage(job_id, "downloaded")

        repack_ok, _, _, unpacked_bytes, repack_reason = await ctx.run_repack_job(
            job_id,
            download_abs,
            callback_payload.get("pack_format", "zip"),
            int(callback_payload.get("pack_level", 3)),
        )
        if not repack_ok:
            if repack_reason == "encrypted_zip":
                try:
                    if os.path.exists(download_abs):
                        os.remove(download_abs)
                except Exception:
                    ctx.logger.warning("failed to cleanup encrypted zip job_id=%s", job_id)
            await ctx.notify_manager(
                {
                    **callback_payload,
                    "job_id": job_id,
                    "status": "error",
                    "reason": repack_reason or "repack_failed",
                }
            )
            await ctx.job_error_cleanup(job_id, repack_reason or "repack_failed")
            return

        await ctx.broadcast(job_id, await ctx.build_state_event(job_id, "complete"))
        callback_success_payload = {
            **callback_payload,
            "job_id": job_id,
            "status": "success",
            "bytes": downloaded,
            "total": total,
        }
        if unpacked_bytes is not None:
            callback_success_payload["unpacked_bytes"] = unpacked_bytes
        await ctx.notify_manager(callback_success_payload)
    except Exception as exc:
        ctx.logger.exception("transfer download failed job_id=%s", job_id)
        try:
            if os.path.exists(download_abs):
                os.remove(download_abs)
        except Exception:
            ctx.logger.warning("failed to cleanup partial file job_id=%s", job_id)
        await update_job_meta(
            ctx,
            job_id,
            {
                "status": "error",
                "error": str(exc),
                "download_completed_at": int(time.time()),
            },
        )
        await ctx.set_state(job_id, status="error", error=str(exc))
        await ctx.broadcast(
            job_id,
            await ctx.build_state_event(job_id, "error", message="download failed"),
        )
        await ctx.notify_manager({**callback_payload, "job_id": job_id, "status": "error", "reason": "exception"})
        await ctx.job_error_cleanup(job_id, "download_exception")
    finally:
        await ctx.close_clients(job_id)
