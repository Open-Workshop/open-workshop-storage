from __future__ import annotations

import time
from typing import Any


JobSnapshot = dict[str, Any]


def new_job_state() -> JobSnapshot:
    return {
        "started": False,
        "status": "pending",
        "stage": "pending",
        "bytes": 0,
        "total": None,
        "percent": None,
        "error": None,
        "clients": [],
        "last_activity": time.time(),
    }


def reset_job_state(
    state: JobSnapshot,
    *,
    started: bool,
    status: str,
    stage: str,
    total: Any = None,
) -> JobSnapshot:
    state.update(
        {
            "started": started,
            "status": status,
            "stage": stage,
            "bytes": 0,
            "total": total,
            "percent": None,
            "error": None,
            "last_activity": time.time(),
        }
    )
    return state


def state_event_payload(
    event: str,
    state: JobSnapshot,
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "event": event,
        "bytes": state.get("bytes", 0),
        "total": state.get("total"),
        "status": state.get("status"),
        "stage": state.get("stage"),
    }
    percent = state.get("percent")
    if percent is not None or event == "progress":
        payload["percent"] = percent
    payload.update(extra)
    return payload
