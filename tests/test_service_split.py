import importlib
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from typing import Any

from fastapi.testclient import TestClient

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def _clear_app_modules() -> None:
    for module_name in list(sys.modules):
        if module_name.startswith("open_workshop_storage"):
            sys.modules.pop(module_name, None)


def _load_configured_module(module_name: str, temp_dir: str):
    config: Any = ModuleType("ow_config")
    config.MAIN_DIR = str(Path(temp_dir) / "storage")
    config.MANAGER_URL = "http://127.0.0.1:7776"
    config.ACCESS_SERVICE_URL = "http://127.0.0.1:7777"
    config.ACCESS_SERVICE_TIMEOUT_SECONDS = 30
    config.MANAGER_TRANSFER_CALLBACK_URL = ""
    config.TRANSFER_JWT_SECRET = "test-secret-with-safe-length-32+"
    config.TRANSFER_CALLBACK_TTL_SECONDS = 600
    config.TRANSFER_MAX_BYTES = 0
    config.TRANSFER_MAX_UNPACKED_BYTES = 0
    config.TRANSFER_UPLOAD_CONCURRENCY = 8
    config.TRANSFER_DOWNLOAD_CONCURRENCY = 16
    config.TRANSFER_REPACK_CONCURRENCY = 8
    config.TRANSFER_UPLOAD_TIMEOUT_SECONDS = 3600
    config.TRANSFER_DOWNLOAD_TIMEOUT_SECONDS = 3600
    config.TRANSFER_CALLBACK_TIMEOUT_SECONDS = 30
    config.SEVEN_ZIP_TIMEOUT_SECONDS = 3600
    config.SEVEN_ZIP_IDLE_TIMEOUT_SECONDS = 60
    config.BLURHASH_CACHE_SIZE = 100000
    config.CLEANUP_INTERVAL_SECONDS = 60
    config.JOB_TTL_SECONDS = 10800
    config.delete_file = "x"
    config.upload_file = "x"
    config.storage_manage_token = "x"

    sys.modules["ow_config"] = config
    _clear_app_modules()
    return importlib.import_module(module_name)


class ServiceSplitTests(unittest.TestCase):
    def test_distributor_app_exposes_only_public_download_routes(self):
        with TemporaryDirectory() as temp_dir:
            distributor = _load_configured_module("open_workshop_storage.distributor", temp_dir)
            distributor.legacy.tools.ensure_7z_available = lambda: None

            with TestClient(distributor.app) as client:
                response = client.get("/distributor/healthz")
                openapi = client.get("/distributor/openapi.json").json()
                self.assertEqual(client.get("/healthz").status_code, 404)
                self.assertEqual(client.get("/openapi.json").status_code, 404)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), {"status": "ok"})
                self.assertIn("/download/{type}/{path}", openapi["paths"])
                self.assertIn("/blurhashes", openapi["paths"])
                self.assertNotIn("/transfer/start", openapi["paths"])
                self.assertNotIn("/upload", openapi["paths"])
                self.assertNotIn("/delete", openapi["paths"])

    def test_loader_app_exposes_only_ingest_and_transfer_routes(self):
        with TemporaryDirectory() as temp_dir:
            loader = _load_configured_module("open_workshop_storage.loader", temp_dir)
            loader.legacy.tools.ensure_7z_available = lambda: None

            async def fake_cleanup_loop() -> None:
                return None

            loader.legacy._cleanup_loop = fake_cleanup_loop

            with TestClient(loader.app) as client:
                response = client.get("/loader/healthz")
                openapi = client.get("/loader/openapi.json").json()
                self.assertEqual(client.get("/healthz").status_code, 404)
                self.assertEqual(client.get("/openapi.json").status_code, 404)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), {"status": "ok"})
                self.assertIn("/upload", openapi["paths"])
                self.assertIn("/delete", openapi["paths"])
                self.assertIn("/transfer/start", openapi["paths"])
                self.assertIn("/transfer/upload", openapi["paths"])
                self.assertIn("/transfer/repack", openapi["paths"])
                self.assertIn("/transfer/move", openapi["paths"])
                self.assertNotIn("/download/{type}/{path}", openapi["paths"])
                self.assertNotIn("/blurhashes", openapi["paths"])
                self.assertTrue(
                    any(getattr(route, "path", None) == "/transfer/ws/{job_id}" for route in loader.app.router.routes)
                )


if __name__ == "__main__":
    unittest.main()
