import asyncio
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from test_transfer_start import _load_main_module


class RepackProgressTests(unittest.TestCase):
    def test_repack_uses_separate_extracting_stage(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            job_id = "c" * 32
            archive_path = Path(main.MAIN_DIR) / "temp" / job_id / "source.zip"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            archive_path.write_bytes(b"archive")
            main._write_meta_sync(job_id, {"job_id": job_id, "download_path": str(archive_path)})

            timeline: list[tuple[str, object]] = []

            async def fake_broadcast(target_job_id, message):
                timeline.append(("broadcast", dict(message)))

            def fake_probe_archive(path):
                return "zip", False, [{"Type": "zip"}]

            def fake_unpacked_bytes(entries):
                return 123

            def fake_zip_uses_deflated_or_better(path, entries=None):
                return False

            def fake_extract_archive(path, dest_dir, entries=None, on_progress=None):
                Path(dest_dir).mkdir(parents=True, exist_ok=True)
                if on_progress is not None:
                    on_progress(20)
                    time.sleep(0.01)
                    on_progress(80)
                (Path(dest_dir) / "content.bin").write_bytes(b"data")

            def fake_zip_dir_with_level(src_dir, dest_zip_path, compresslevel=3, on_progress=None):
                timeline.append(("zip-start", None))
                if on_progress is not None:
                    on_progress(10)
                    on_progress(100)
                Path(dest_zip_path).write_bytes(b"packed")

            original_broadcast = main._broadcast
            original_probe_archive = main.tools.probe_archive
            original_unpacked_bytes = main.tools.archive_entries_unpacked_bytes
            original_zip_uses_deflated_or_better = main.tools.zip_uses_deflated_or_better
            original_extract_archive = main.tools.safe_extract_archive
            original_zip_dir_with_level = main.tools.zip_dir_with_level
            original_progress_interval = main.PROGRESS_PUSH_INTERVAL

            main._broadcast = fake_broadcast
            main.tools.probe_archive = fake_probe_archive
            main.tools.archive_entries_unpacked_bytes = fake_unpacked_bytes
            main.tools.zip_uses_deflated_or_better = fake_zip_uses_deflated_or_better
            main.tools.safe_extract_archive = fake_extract_archive
            main.tools.zip_dir_with_level = fake_zip_dir_with_level
            main.PROGRESS_PUSH_INTERVAL = 0.0
            try:
                ok, _, _, unpacked_bytes, reason = asyncio.run(
                    main._run_repack_job(job_id, str(archive_path), "zip", 3)
                )
            finally:
                main._broadcast = original_broadcast
                main.tools.probe_archive = original_probe_archive
                main.tools.archive_entries_unpacked_bytes = original_unpacked_bytes
                main.tools.zip_uses_deflated_or_better = original_zip_uses_deflated_or_better
                main.tools.safe_extract_archive = original_extract_archive
                main.tools.zip_dir_with_level = original_zip_dir_with_level
                main.PROGRESS_PUSH_INTERVAL = original_progress_interval

            self.assertTrue(ok)
            self.assertEqual(unpacked_bytes, 123)
            self.assertIsNone(reason)

            zip_start_index = next(
                index for index, item in enumerate(timeline) if item[0] == "zip-start"
            )
            extract_progress_before_zip = [
                item[1]
                for item in timeline[:zip_start_index]
                if item[0] == "broadcast"
                and item[1].get("event") == "progress"
                and item[1].get("stage") == "extracting"
                and (item[1].get("percent") or 0) > 0
            ]
            self.assertTrue(
                extract_progress_before_zip,
                "expected extracting progress before zip stage starts",
            )

            repack_progress_after_zip = [
                item[1]
                for item in timeline[zip_start_index:]
                if item[0] == "broadcast"
                and item[1].get("event") == "progress"
                and item[1].get("stage") == "repacking"
                and (item[1].get("percent") or 0) > 0
            ]
            self.assertTrue(
                repack_progress_after_zip,
                "expected repacking progress after zip stage starts",
            )


if __name__ == "__main__":
    unittest.main()
