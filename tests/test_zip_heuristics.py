import importlib
import sys
import unittest
from pathlib import Path
from types import ModuleType

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def _clear_app_modules() -> None:
    for module_name in list(sys.modules):
        if module_name.startswith("open_workshop_storage"):
            sys.modules.pop(module_name, None)


def _load_tools_module():
    config = ModuleType("ow_config")
    config.MAIN_DIR = "/tmp/storage-tests"
    config.MANAGER_URL = "http://127.0.0.1:7776"
    config.MANAGER_TRANSFER_CALLBACK_URL = ""
    config.TRANSFER_JWT_SECRET = "test-secret-with-safe-length-32+"
    config.TRANSFER_CALLBACK_TTL_SECONDS = 600
    config.TRANSFER_MAX_BYTES = 0

    sys.modules["ow_config"] = config
    sys.modules["bcrypt"] = ModuleType("bcrypt")
    sys.modules["jwt"] = ModuleType("jwt")
    pil_module = ModuleType("PIL")
    pil_module.Image = object
    pil_module.UnidentifiedImageError = RuntimeError
    sys.modules["PIL"] = pil_module
    _clear_app_modules()
    return importlib.import_module("open_workshop_storage.utils")


class ZipHeuristicsTests(unittest.TestCase):
    def test_allows_mixed_store_and_deflate_when_total_savings_is_meaningful(self):
        tools = _load_tools_module()
        entries = [
            {"Type": "zip"},
            {
                "Path": "archive/random.bin",
                "Folder": "-",
                "Size": "100000",
                "Packed Size": "100000",
                "Method": "Store",
            },
            {
                "Path": "archive/text.txt",
                "Folder": "-",
                "Size": "100000",
                "Packed Size": "341",
                "Method": "Deflate",
            },
        ]

        self.assertTrue(tools.zip_uses_deflated_or_better("unused.zip", entries))

    def test_rejects_zip_when_total_savings_is_below_threshold(self):
        tools = _load_tools_module()
        entries = [
            {"Type": "zip"},
            {
                "Path": "archive/a.bin",
                "Folder": "-",
                "Size": "100000",
                "Packed Size": "99550",
                "Method": "Deflate",
            },
            {
                "Path": "archive/b.bin",
                "Folder": "-",
                "Size": "100000",
                "Packed Size": "99550",
                "Method": "Store",
            },
        ]

        self.assertFalse(tools.zip_uses_deflated_or_better("unused.zip", entries))

    def test_probe_archive_uses_facade_ensure_7z_available_override(self):
        tools = _load_tools_module()
        import open_workshop_storage.utils.archive as archive_tools

        original_run = archive_tools.subprocess.run
        original_ensure = tools.ensure_7z_available

        def fail_run(*args, **kwargs):
            raise AssertionError("subprocess.run should not be called when facade override fails first")

        def fake_ensure():
            raise RuntimeError("patched ensure")

        archive_tools.subprocess.run = fail_run
        tools.ensure_7z_available = fake_ensure
        try:
            with self.assertRaisesRegex(RuntimeError, "patched ensure"):
                tools.probe_archive("unused.zip")
        finally:
            archive_tools.subprocess.run = original_run
            tools.ensure_7z_available = original_ensure


if __name__ == "__main__":
    unittest.main()
