import importlib
import sys
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from typing import Any

import aiohttp
from fastapi.testclient import TestClient

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def _clear_app_modules() -> None:
    for module_name in list(sys.modules):
        if module_name.startswith("open_workshop_storage"):
            sys.modules.pop(module_name, None)


def _load_main_module(temp_dir: str):
    config: Any = ModuleType("ow_config")
    config.MAIN_DIR = str(Path(temp_dir) / "storage")
    config.MANAGER_URL = "http://127.0.0.1:8000/api/manager"
    config.ACCESS_SERVICE_URL = "http://127.0.0.1:8001/api/access"
    config.ACCESS_SERVICE_TIMEOUT_SECONDS = 30
    config.MANAGER_TRANSFER_CALLBACK_URL = ""
    config.TRANSFER_JWT_SECRET = "test-secret-with-safe-length-32+"
    config.TRANSFER_CALLBACK_TTL_SECONDS = 600
    config.TRANSFER_MAX_BYTES = 0

    sys.modules["ow_config"] = config
    _clear_app_modules()

    main = importlib.import_module("open_workshop_storage.app")
    main.tools.ensure_7z_available = lambda: None
    return main


def _make_token(main_module, job_id: str) -> str:
    token = main_module.tools.encode_transfer_jwt(
        {
            "job_id": job_id,
            "download_url": "https://example.com/archive.zip",
            "filename": "archive.zip",
        },
        audience="storage",
        ttl_seconds=60,
    )
    if not token:
        raise AssertionError("failed to create transfer token for test")
    return token


class TransferStartTests(unittest.TestCase):
    def test_transfer_start_runs_even_if_placeholder_state_exists(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            job_id = "a" * 32
            main.JOB_STATE.clear()
            main.JOB_STATE[job_id] = main._new_job_state()

            called = threading.Event()
            started_jobs: list[str] = []

            async def fake_run_download_job(
                started_job_id,
                download_url,
                download_abs,
                max_bytes,
                callback_payload,
            ) -> None:
                started_jobs.append(started_job_id)
                called.set()

            original = main._run_download_job
            main._run_download_job = fake_run_download_job
            try:
                with TestClient(main.app) as client:
                    response = client.get(
                        "/transfer/start",
                        params={"token": _make_token(main, job_id)},
                    )
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json()["status"], "started")
                    self.assertTrue(called.wait(1.0))
            finally:
                main._run_download_job = original

            self.assertEqual(started_jobs, [job_id])
            self.assertTrue(main.JOB_STATE[job_id]["started"])

    def test_transfer_start_does_not_duplicate_started_job(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            job_id = "b" * 32
            main.JOB_STATE.clear()
            state = main._new_job_state()
            state.update({"started": True, "status": "downloading", "stage": "downloading"})
            main.JOB_STATE[job_id] = state

            called = threading.Event()

            async def fake_run_download_job(
                started_job_id,
                download_url,
                download_abs,
                max_bytes,
                callback_payload,
            ) -> None:
                called.set()

            original = main._run_download_job
            main._run_download_job = fake_run_download_job
            try:
                with TestClient(main.app) as client:
                    response = client.get(
                        "/transfer/start",
                        params={"token": _make_token(main, job_id)},
                    )
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json()["status"], "downloading")
                    self.assertFalse(called.wait(0.2))
            finally:
                main._run_download_job = original

    def test_download_mod_returns_503_when_access_service_is_unreachable(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            archive_path = Path(main.MAIN_DIR) / "archive" / "mods" / "123" / "main.zip"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            archive_path.write_bytes(b"archive")

            import open_workshop_storage.services.access_client as access_client

            class FailingSession:
                def __init__(self, *args, **kwargs):
                    pass

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                def post(self, *args, **kwargs):
                    raise aiohttp.ClientError("access service offline")

            original_session = access_client.aiohttp.ClientSession
            access_client.aiohttp.ClientSession = FailingSession
            try:
                with TestClient(main.app) as client:
                    response = client.get("/download/archive/mods/123/main.zip")
            finally:
                access_client.aiohttp.ClientSession = original_session

            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.text, "Access service unavailable")

    def test_download_mod_queries_access_service(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            archive_path = Path(main.MAIN_DIR) / "archive" / "mods" / "123" / "main.zip"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            archive_path.write_bytes(b"archive")

            import open_workshop_storage.services.access_client as access_client

            captured_request: dict[str, object] = {}

            class FakeResponse:
                status = 200

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                async def json(self):
                    return {"download": {"value": True, "reason": "allowed", "reason_code": "public"}}

            class FakeSession:
                def __init__(self, *args, **kwargs):
                    pass

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                def post(self, url, **kwargs):
                    captured_request["url"] = url
                    captured_request["kwargs"] = kwargs
                    return FakeResponse()

            original_session = access_client.aiohttp.ClientSession
            access_client.aiohttp.ClientSession = FakeSession
            try:
                with TestClient(main.app) as client:
                    response = client.get(
                        "/download/archive/mods/123/main.zip",
                        cookies={
                            "accessToken": "access-token",
                            "refreshToken": "refresh-token",
                            "userID": "123",
                        },
                    )
            finally:
                access_client.aiohttp.ClientSession = original_session

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.content, b"archive")
            self.assertEqual(
                captured_request["url"],
                "http://127.0.0.1:8001/api/access/mod/123",
            )
            self.assertEqual(captured_request["kwargs"]["json"], {})
            self.assertEqual(
                captured_request["kwargs"]["cookies"],
                {
                    "accessToken": "access-token",
                    "refreshToken": "refresh-token",
                },
            )

    def test_download_mod_returns_403_when_access_is_denied(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            archive_path = Path(main.MAIN_DIR) / "archive" / "mods" / "123" / "main.zip"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            archive_path.write_bytes(b"archive")

            import open_workshop_storage.services.access_client as access_client

            class DenyResponse:
                status = 200

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                async def json(self):
                    return {"download": {"value": False, "reason": "denied", "reason_code": "hidden"}}

            class DenySession:
                def __init__(self, *args, **kwargs):
                    pass

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                def post(self, url, **kwargs):
                    return DenyResponse()

            original_session = access_client.aiohttp.ClientSession
            access_client.aiohttp.ClientSession = DenySession
            try:
                with TestClient(main.app) as client:
                    response = client.get("/download/archive/mods/123/main.zip")
            finally:
                access_client.aiohttp.ClientSession = original_session

            self.assertEqual(response.status_code, 403)
            self.assertEqual(response.text, "denied")

    def test_download_mod_surfaces_access_service_error_message(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            archive_path = Path(main.MAIN_DIR) / "archive" / "mods" / "123" / "main.zip"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            archive_path.write_bytes(b"archive")

            import open_workshop_storage.services.access_client as access_client

            class ErrorResponse:
                status = 403

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                async def text(self):
                    return "У вас нет доступа к этому моду."

            class ErrorSession:
                def __init__(self, *args, **kwargs):
                    pass

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                def post(self, url, **kwargs):
                    return ErrorResponse()

            original_session = access_client.aiohttp.ClientSession
            access_client.aiohttp.ClientSession = ErrorSession
            try:
                with TestClient(main.app) as client:
                    response = client.get("/download/archive/mods/123/main.zip")
            finally:
                access_client.aiohttp.ClientSession = original_session

            self.assertEqual(response.status_code, 403)
            self.assertEqual(response.text, "У вас нет доступа к этому моду.")


if __name__ == "__main__":
    unittest.main()
