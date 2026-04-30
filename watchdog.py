#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import dataclass
from typing import Mapping
from urllib import error, request

DEFAULT_CHECK_INTERVAL_SECONDS = 20.0
DEFAULT_RESTART_AFTER_SECONDS = 300.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 5.0

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
logger = logging.getLogger("open_workshop.watchdog")


@dataclass(frozen=True)
class WatchdogSettings:
    health_url: str
    restart_command: str
    check_interval_seconds: float = DEFAULT_CHECK_INTERVAL_SECONDS
    restart_after_seconds: float = DEFAULT_RESTART_AFTER_SECONDS
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS


@dataclass
class FailureTracker:
    since: float | None = None

    def observe(self, healthy: bool, now: float, restart_after_seconds: float) -> str:
        if healthy:
            was_unhealthy = self.since is not None
            self.since = None
            return "recovered" if was_unhealthy else "healthy"

        if self.since is None:
            self.since = now
            return "first_failure"

        if now - self.since >= restart_after_seconds:
            self.since = None
            return "restart"

        return "waiting"


@dataclass(frozen=True)
class ProbeResult:
    healthy: bool
    detail: str


def _read_float_setting(
    env: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    raw_value = env.get(name, "").strip()
    if not raw_value:
        return default

    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive number") from exc

    if value <= 0:
        raise ValueError(f"{name} must be a positive number")

    return value


def load_settings(env: Mapping[str, str] | None = None) -> WatchdogSettings:
    env = os.environ if env is None else env

    health_url = env.get("WATCHDOG_HEALTH_URL", "").strip()
    if not health_url:
        raise ValueError("WATCHDOG_HEALTH_URL is required")

    restart_command = env.get("WATCHDOG_RESTART_COMMAND", "").strip()
    if not restart_command:
        raise ValueError("WATCHDOG_RESTART_COMMAND is required")

    return WatchdogSettings(
        health_url=health_url,
        restart_command=restart_command,
        check_interval_seconds=_read_float_setting(
            env,
            "WATCHDOG_CHECK_INTERVAL_SECONDS",
            DEFAULT_CHECK_INTERVAL_SECONDS,
        ),
        restart_after_seconds=_read_float_setting(
            env,
            "WATCHDOG_RESTART_AFTER_SECONDS",
            DEFAULT_RESTART_AFTER_SECONDS,
        ),
        request_timeout_seconds=_read_float_setting(
            env,
            "WATCHDOG_REQUEST_TIMEOUT_SECONDS",
            DEFAULT_REQUEST_TIMEOUT_SECONDS,
        ),
    )


def probe_health(health_url: str, timeout_seconds: float) -> ProbeResult:
    health_request = request.Request(
        health_url,
        method="GET",
        headers={"User-Agent": "open-workshop-watchdog/1.0"},
    )

    try:
        with request.urlopen(health_request, timeout=timeout_seconds) as response:
            status = getattr(response, "status", response.getcode())
            if 200 <= status < 400:
                return ProbeResult(True, f"status={status}")
            return ProbeResult(False, f"status={status}")
    except error.HTTPError as exc:
        return ProbeResult(False, f"http {exc.code}")
    except error.URLError as exc:
        return ProbeResult(False, f"url error: {exc.reason}")
    except TimeoutError:
        return ProbeResult(False, "timeout")
    except Exception as exc:  # pragma: no cover - defensive fallback
        return ProbeResult(False, f"{exc.__class__.__name__}: {exc}")


def restart_service(restart_command: str) -> int:
    completed = subprocess.run(restart_command, shell=True, check=False)
    return completed.returncode


def run_watchdog(
    settings: WatchdogSettings,
    *,
    probe=probe_health,
    restarter=restart_service,
    sleeper=time.sleep,
    monotonic=time.monotonic,
) -> None:
    tracker = FailureTracker()
    logger.info(
        "watchdog started: health_url=%s check_interval=%.1fs restart_after=%.1fs request_timeout=%.1fs",
        settings.health_url,
        settings.check_interval_seconds,
        settings.restart_after_seconds,
        settings.request_timeout_seconds,
    )

    while True:
        result = probe(settings.health_url, settings.request_timeout_seconds)
        now = monotonic()
        decision = tracker.observe(result.healthy, now, settings.restart_after_seconds)

        if decision == "healthy":
            logger.debug("health check passed: %s", result.detail)
        elif decision == "recovered":
            logger.info("health check recovered: %s", result.detail)
        elif decision == "first_failure":
            logger.warning(
                "health check failed: %s; will restart after %.1fs of continuous failure",
                result.detail,
                settings.restart_after_seconds,
            )
        elif decision == "restart":
            logger.error(
                "health check failed for %.1fs; restarting with command: %s",
                settings.restart_after_seconds,
                settings.restart_command,
            )
            exit_code = restarter(settings.restart_command)
            if exit_code != 0:
                logger.error("restart command exited with code %s", exit_code)
        sleeper(settings.check_interval_seconds)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    try:
        settings = load_settings()
    except ValueError as exc:
        logger.error("%s", exc)
        return 2

    try:
        run_watchdog(settings)
    except KeyboardInterrupt:
        logger.info("watchdog stopped")
        return 130

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
