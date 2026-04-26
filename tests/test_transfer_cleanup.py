import asyncio
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from test_transfer_start import _load_main_module, _make_token


class TransferCleanupTests(unittest.TestCase):
    def test_transfer_move_releases_job_state_and_closes_clients(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            job_id = "c" * 32
            packed_rel = f"temp/{job_id}/packed.zip"
            packed_abs = Path(main.MAIN_DIR) / packed_rel
            packed_abs.parent.mkdir(parents=True, exist_ok=True)
            packed_abs.write_bytes(b"packed")
            main._write_meta_sync(job_id, {"job_id": job_id, "packed_path": packed_rel})

            closed = threading.Event()

            class FakeWebSocket:
                async def close(self):
                    closed.set()

            state = main._new_job_state()
            state.update(
                {
                    "started": True,
                    "status": "packed",
                    "stage": "packed",
                    "clients": [FakeWebSocket()],
                }
            )
            main.JOB_STATE[job_id] = state

            original_check_token = main.tools.check_token
            main.tools.check_token = lambda token_name, token: True
            try:
                with TestClient(main.app) as client:
                    response = client.post(
                        "/transfer/move",
                        data={
                            "job_id": job_id,
                            "type": "archive",
                            "path": "mods/123/main.zip",
                            "token": "token",
                        },
                    )
            finally:
                main.tools.check_token = original_check_token

            self.assertEqual(response.status_code, 200)
            self.assertFalse((Path(main.MAIN_DIR) / "temp" / job_id).exists())
            self.assertNotIn(job_id, main.JOB_STATE)
            self.assertTrue(closed.wait(1.0))

    def test_transfer_ws_discards_placeholder_state_on_disconnect(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            job_id = "d" * 32
            token = _make_token(main, job_id)
            main.JOB_STATE.clear()

            with TestClient(main.app) as client:
                with client.websocket_connect(f"/transfer/ws/{job_id}?token={token}") as ws:
                    snapshot = ws.receive_json()

            self.assertEqual(snapshot["stage"], "pending")
            self.assertNotIn(job_id, main.JOB_STATE)

    def test_broadcast_prunes_failed_clients(self):
        with TemporaryDirectory() as temp_dir:
            main = _load_main_module(temp_dir)
            job_id = "e" * 32

            closed = threading.Event()

            class DeadWebSocket:
                async def send_json(self, message):
                    raise RuntimeError("disconnected")

                async def close(self):
                    closed.set()

            state = main._new_job_state()
            state.update({"started": True, "clients": [DeadWebSocket()]})
            main.JOB_STATE[job_id] = state

            asyncio.run(main._broadcast(job_id, {"event": "progress"}))

            self.assertEqual(main.JOB_STATE[job_id]["clients"], [])
            self.assertTrue(closed.wait(1.0))


if __name__ == "__main__":
    unittest.main()
