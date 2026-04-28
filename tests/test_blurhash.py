import importlib
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from typing import Any

from fastapi.testclient import TestClient
from PIL import Image

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
    config.MANAGER_URL = "http://127.0.0.1:7776"
    config.ACCESS_SERVICE_URL = "http://127.0.0.1:7777"
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


class BlurhashEndpointTests(unittest.TestCase):
    def test_blurhash_batch_generates_hashes_for_download_urls(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            image_path = Path(main.MAIN_DIR) / "resource" / "mods" / "123" / "logo.png"
            image_path.parent.mkdir(parents=True, exist_ok=True)
            Image.new("RGB", (6, 4), (255, 64, 0)).save(image_path, format="PNG")

            source_url = "https://storage.openworkshop.miskler.ru/download/resource/mods/123/logo.png"
            with TestClient(main.app) as client:
                response = client.post("/blurhashes", json={"paths": [source_url]})

            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(len(payload["items"]), 1)
            item = payload["items"][0]
            self.assertEqual(item["path"], source_url)
            self.assertIsInstance(item["blurhash"], str)
            self.assertEqual(len(item["blurhash"]), 28)
            self.assertEqual(item["width"], 6)
            self.assertEqual(item["height"], 4)

    def test_blurhash_batch_uses_cache_for_repeated_files(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            import open_workshop_storage.api.routes.files as files

            image_path = Path(main.MAIN_DIR) / "resource" / "mods" / "123" / "logo.png"
            image_path.parent.mkdir(parents=True, exist_ok=True)
            Image.new("RGB", (6, 4), (255, 64, 0)).save(image_path, format="PNG")

            files._blurhash_for_file.cache_clear()
            original = files.image_file_to_blurhash
            call_count = {"value": 0}

            def fake_image_file_to_blurhash(*args, **kwargs):
                call_count["value"] += 1
                return original(*args, **kwargs)

            files.image_file_to_blurhash = fake_image_file_to_blurhash
            try:
                with TestClient(main.app) as client:
                    for _ in range(2):
                        response = client.post(
                            "/blurhashes",
                            json={
                                "paths": [
                                    "https://storage.openworkshop.miskler.ru/download/resource/mods/123/logo.png"
                                ]
                            },
                        )
                        self.assertEqual(response.status_code, 200)

                self.assertEqual(call_count["value"], 1)
                self.assertEqual(files._blurhash_for_file.cache_info().maxsize, 100000)
            finally:
                files.image_file_to_blurhash = original
                files._blurhash_for_file.cache_clear()

    def test_blurhash_batch_deduplicates_repeated_paths_in_one_request(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            import open_workshop_storage.api.routes.files as files

            image_path = Path(main.MAIN_DIR) / "resource" / "mods" / "123" / "logo.png"
            image_path.parent.mkdir(parents=True, exist_ok=True)
            Image.new("RGB", (6, 4), (255, 64, 0)).save(image_path, format="PNG")

            files._blurhash_for_file.cache_clear()
            original = files.image_file_to_blurhash
            call_count = {"value": 0}

            def fake_image_file_to_blurhash(*args, **kwargs):
                call_count["value"] += 1
                return original(*args, **kwargs)

            files.image_file_to_blurhash = fake_image_file_to_blurhash
            try:
                with TestClient(main.app) as client:
                    response = client.post(
                        "/blurhashes",
                        json={
                            "paths": [
                                "https://storage.openworkshop.miskler.ru/download/resource/mods/123/logo.png",
                                "https://storage.openworkshop.miskler.ru/download/resource/mods/123/logo.png",
                            ]
                        },
                    )

                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertEqual(len(payload["items"]), 2)
                self.assertEqual(payload["items"][0]["blurhash"], payload["items"][1]["blurhash"])
                self.assertEqual(call_count["value"], 1)
            finally:
                files.image_file_to_blurhash = original
                files._blurhash_for_file.cache_clear()

    def test_blurhash_batch_supports_cors_preflight(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            with TestClient(main.app) as client:
                response = client.options(
                    "/blurhashes",
                    headers={
                        "Origin": "https://openworkshop.miskler.ru",
                        "Access-Control-Request-Method": "POST",
                        "Access-Control-Request-Headers": "content-type",
                    },
                )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers.get("access-control-allow-origin"), "*")
