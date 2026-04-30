import asyncio
import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from test_transfer_start import _load_main_module


def _make_upload_token(main_module, job_id: str) -> str:
    token = main_module.tools.encode_transfer_jwt(
        {
            "job_id": job_id,
            "transfer_kind": "archive",
            "mod_id": 123,
            "pack_format": "zip",
            "pack_level": 3,
        },
        audience="storage",
        ttl_seconds=60,
    )
    if not token:
        raise AssertionError("failed to create upload token for test")
    return token


class TransferSafetyTests(unittest.TestCase):
    def test_transfer_upload_returns_429_when_upload_limiter_is_full(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            job_id = "f" * 32
            token = _make_upload_token(main, job_id)
            callbacks: list[dict[str, object]] = []

            async def fake_notify_manager(payload):
                callbacks.append(dict(payload))

            original_active = main.UPLOAD_LIMITER._active
            original_notify_manager = main._notify_manager
            main.UPLOAD_LIMITER._active = main.UPLOAD_LIMITER.limit
            main._notify_manager = fake_notify_manager
            try:
                with TestClient(main.app) as client:
                    response = client.post(
                        "/transfer/upload",
                        headers={
                            "Authorization": f"Bearer {token}",
                            "X-File-Name": "archive.zip",
                        },
                        content=b"archive",
                    )
            finally:
                main.UPLOAD_LIMITER._active = original_active
                main._notify_manager = original_notify_manager

            self.assertEqual(response.status_code, 429)
            self.assertEqual(response.text, "Storage busy")
            self.assertEqual(callbacks[-1]["status"], "error")
            self.assertEqual(callbacks[-1]["reason"], "busy")

    def test_transfer_upload_timeout_releases_job(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            main.config.TRANSFER_UPLOAD_TIMEOUT_SECONDS = 0.01
            job_id = "9" * 32
            token = _make_upload_token(main, job_id)
            callbacks: list[dict[str, object]] = []

            async def fake_notify_manager(payload):
                callbacks.append(dict(payload))

            async def slow_repack_job(*args, **kwargs):
                await asyncio.sleep(1)
                return True, None, None, None, None

            original_notify_manager = main._notify_manager
            original_run_repack_job = main._run_repack_job
            main._notify_manager = fake_notify_manager
            main._run_repack_job = slow_repack_job
            try:
                with TestClient(main.app) as client:
                    response = client.post(
                        "/transfer/upload",
                        headers={
                            "Authorization": f"Bearer {token}",
                            "X-File-Name": "archive.zip",
                        },
                        content=b"archive",
                    )
            finally:
                main._notify_manager = original_notify_manager
                main._run_repack_job = original_run_repack_job

            self.assertEqual(response.status_code, 408)
            self.assertEqual(response.text, "Upload timed out")
            self.assertNotIn(job_id, main.JOB_STATE)
            self.assertFalse((Path(main.MAIN_DIR) / "temp" / job_id).exists())
            self.assertEqual(callbacks[-1]["status"], "error")
            self.assertEqual(callbacks[-1]["reason"], "timeout")

    def test_repack_rejects_archive_over_unpacked_size_limit(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            main.config.TRANSFER_MAX_UNPACKED_BYTES = 100
            job_id = "a" * 32
            archive_path = Path(main.MAIN_DIR) / "temp" / job_id / "source.zip"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            archive_path.write_bytes(b"archive")
            main._write_meta_sync(job_id, {"job_id": job_id, "download_path": str(archive_path)})

            def fake_probe_archive(path):
                return "zip", False, [{"Type": "zip"}, {"Path": "content.bin", "Size": "101"}]

            def fake_unpacked_bytes(entries):
                return 101

            original_probe_archive = main.tools.probe_archive
            original_unpacked_bytes = main.tools.archive_entries_unpacked_bytes
            main.tools.probe_archive = fake_probe_archive
            main.tools.archive_entries_unpacked_bytes = fake_unpacked_bytes
            try:
                ok, _, _, unpacked_bytes, reason = asyncio.run(
                    main._run_repack_job(job_id, str(archive_path), "zip", 3)
                )
            finally:
                main.tools.probe_archive = original_probe_archive
                main.tools.archive_entries_unpacked_bytes = original_unpacked_bytes

            self.assertFalse(ok)
            self.assertEqual(unpacked_bytes, 101)
            self.assertEqual(reason, "unpacked_size_limit")
            self.assertEqual(main.JOB_STATE[job_id]["error"], "unpacked_size_limit")

    def test_download_job_reports_busy_when_download_limiter_is_full(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            job_id = "8" * 32
            download_path = Path(main.MAIN_DIR) / "temp" / job_id / "source.zip"
            download_path.parent.mkdir(parents=True, exist_ok=True)
            main._write_meta_sync(job_id, {"job_id": job_id, "download_path": str(download_path)})
            callbacks: list[dict[str, object]] = []

            async def fake_notify_manager(payload):
                callbacks.append(dict(payload))

            original_active = main.DOWNLOAD_LIMITER._active
            original_notify_manager = main._notify_manager
            main.DOWNLOAD_LIMITER._active = main.DOWNLOAD_LIMITER.limit
            main._notify_manager = fake_notify_manager
            try:
                asyncio.run(
                    main._run_download_job(
                        job_id,
                        "https://example.com/archive.zip",
                        str(download_path),
                        None,
                        {"mod_id": 123},
                    )
                )
            finally:
                main.DOWNLOAD_LIMITER._active = original_active
                main._notify_manager = original_notify_manager

            self.assertNotIn(job_id, main.JOB_STATE)
            self.assertFalse((Path(main.MAIN_DIR) / "temp" / job_id).exists())
            self.assertEqual(callbacks[-1]["status"], "error")
            self.assertEqual(callbacks[-1]["reason"], "busy")

    def test_7z_helpers_raise_on_timeout(self):
        from open_workshop_storage.utils import archive

        original_bin = archive.SEVEN_ZIP_BIN
        archive.SEVEN_ZIP_BIN = sys.executable
        try:
            with self.assertRaises(archive.SevenZipTimeoutError) as total_timeout_exc:
                archive._run_7z(
                    ["-c", "import time; time.sleep(2)"],
                    timeout_seconds=0.1,
                )
            self.assertIn("0.1s", str(total_timeout_exc.exception))

            started_at = time.monotonic()
            with self.assertRaises(archive.SevenZipTimeoutError) as idle_timeout_exc:
                archive._run_7z_with_progress(
                    ["-c", "import time; time.sleep(2)"],
                    timeout_seconds=2,
                    idle_timeout_seconds=0.1,
                )
            self.assertLess(time.monotonic() - started_at, 2)
            self.assertIn("idle", str(idle_timeout_exc.exception))
            self.assertIn("0.1s", str(idle_timeout_exc.exception))
        finally:
            archive.SEVEN_ZIP_BIN = original_bin


if __name__ == "__main__":
    unittest.main()
