from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import aiohttp
from fastapi import Request


@dataclass(frozen=True)
class ModDownloadAccessResult:
    allowed: bool
    reason: str
    reason_code: str


class AccessServiceError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_text: str | None = None


def _session_cookies(request: Request) -> dict[str, str]:
    cookies: dict[str, str] = {}

    access_token = request.cookies.get("accessToken", "")
    if access_token:
        cookies["accessToken"] = access_token

    refresh_token = request.cookies.get("refreshToken", "")
    if refresh_token:
        cookies["refreshToken"] = refresh_token

    return cookies


def _extract_error_message(response_text: str) -> str:
    text = response_text.strip()
    if not text:
        return "Access service unavailable"

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text

    if isinstance(payload, dict):
        for key in ("detail", "message", "reason", "title"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return text

    if isinstance(payload, str) and payload.strip():
        return payload.strip()

    return text


async def resolve_mod_download_access(
    *,
    request: Request,
    mod_id: int,
    access_service_url: str,
    timeout_seconds: int,
) -> ModDownloadAccessResult:
    url = access_service_url.rstrip("/") + f"/mod/{mod_id}"
    timeout = aiohttp.ClientTimeout(total=float(timeout_seconds))

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                url,
                json={},
                cookies=_session_cookies(request) or None,
            ) as resp:
                if resp.status != 200:
                    response_text = await resp.text()
                    error = AccessServiceError(
                        _extract_error_message(response_text),
                        status_code=resp.status,
                    )
                    error.response_text = response_text
                    raise error
                payload = await resp.json()
    except aiohttp.ContentTypeError as exc:
        raise AccessServiceError("Access service returned invalid JSON", status_code=502) from exc
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        raise AccessServiceError("Access service unavailable", status_code=503) from exc
    except ValueError as exc:
        raise AccessServiceError("Access service returned invalid JSON", status_code=502) from exc

    if not isinstance(payload, dict):
        raise AccessServiceError("Access service returned unexpected response", status_code=502)

    download = payload.get("download")
    if not isinstance(download, dict):
        raise AccessServiceError("Access service returned unexpected response", status_code=502)

    allowed = bool(download.get("value"))
    reason = download.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        reason = "Мод доступен для скачивания." if allowed else "Access denied"
    reason_code = download.get("reason_code")
    if not isinstance(reason_code, str) or not reason_code.strip():
        reason_code = "public" if allowed else "forbidden"

    return ModDownloadAccessResult(
        allowed=allowed,
        reason=reason,
        reason_code=reason_code,
    )
