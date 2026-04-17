import importlib
import sys
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType

from fastapi.testclient import TestClient
import aiohttp


def _load_main_module(temp_dir: str):
    config = ModuleType("ow_config")
    config.MAIN_DIR = str(Path(temp_dir) / "storage")
    config.MANAGER_URL = "http://127.0.0.1:8000/api/manager"
    config.MANAGER_TRANSFER_CALLBACK_URL = ""
    config.TRANSFER_JWT_SECRET = "test-secret-with-safe-length-32+"
    config.TRANSFER_CALLBACK_TTL_SECONDS = 600
    config.TRANSFER_MAX_BYTES = 0
    config.check_access = "test-manager-token"

    sys.modules["ow_config"] = config
    sys.modules.pop("tools", None)
    sys.modules.pop("main", None)

    main = importlib.import_module("main")
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

    def test_download_mod_returns_503_when_manager_is_unreachable(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            archive_path = Path(main.MAIN_DIR) / "archive" / "mods" / "123" / "main.zip"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            archive_path.write_bytes(b"archive")

            import storage_service.file_routes as file_routes

            class FailingSession:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                def get(self, *args, **kwargs):
                    raise aiohttp.ClientError("manager offline")

            original_session = file_routes.aiohttp.ClientSession
            file_routes.aiohttp.ClientSession = FailingSession
            try:
                with TestClient(main.app) as client:
                    response = client.get("/download/archive/mods/123/main.zip")
            finally:
                file_routes.aiohttp.ClientSession = original_session

            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.text, "Manager unavailable")


if __name__ == "__main__":
    unittest.main()
