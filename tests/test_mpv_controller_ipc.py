import threading
import unittest
from unittest import mock

import kiosk


def controller_config() -> dict:
    return {
        "mpv_path": "mpv",
        "ipc_path": "/tmp/kiosky-test/mpv.sock",
        "runtime_dir": "/tmp/kiosky-test",
        "mpv_log_file": "/tmp/kiosky-test/mpv.log",
        "mpv_msg_level": "",
        "mpv_ipc_timeout_sec": 0.2,
        "mpv_startup_timeout_sec": 0.2,
        "mpv_debug_events": False,
        "hotkeys_enabled": False,
        "lock_input": True,
        "low_resource_mode": False,
        "hwdec": "",
        "rotation_deg": 0,
    }


class BlockingIPC:
    def __init__(self) -> None:
        self.write_entered = threading.Event()
        self.release_write = threading.Event()
        self.close_called = threading.Event()
        self.closed = False
        self.bad_file_descriptor = False
        self.write_calls = 0

    def write(self, data: bytes) -> int:
        self.write_entered.set()
        if not self.release_write.wait(timeout=1.0):
            raise AssertionError("test timed out waiting to release IPC write")
        if self.closed:
            self.bad_file_descriptor = True
            raise OSError("Bad file descriptor")
        self.write_calls += 1
        return len(data)

    def flush(self) -> None:
        if self.closed:
            self.bad_file_descriptor = True
            raise OSError("Bad file descriptor")

    def close(self) -> None:
        self.closed = True
        self.close_called.set()


class ImmediateIPC:
    def __init__(self) -> None:
        self.closed = False
        self.write_calls = 0

    def write(self, data: bytes) -> int:
        self.write_calls += 1
        return len(data)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class ChunkedSocket:
    def __init__(self, chunks) -> None:
        self.chunks = list(chunks)
        self.timeout_values = []
        self.sent = []
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        self.timeout_values.append(timeout)

    def recv(self, _size: int) -> bytes:
        if not self.chunks:
            return b""
        chunk = self.chunks.pop(0)
        if isinstance(chunk, BaseException):
            raise chunk
        return chunk

    def sendall(self, data: bytes) -> None:
        self.sent.append(data)

    def close(self) -> None:
        self.closed = True


class FakeProcess:
    def __init__(self, pid: int = 4321) -> None:
        self.pid = pid
        self.wait_called = False
        self.terminated = False
        self.killed = False

    def poll(self):
        return None

    def wait(self, timeout=None):
        self.wait_called = True
        return 0

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


class MPVControllerIPCLockTests(unittest.TestCase):
    def build_controller(self, ipc, proc=None) -> kiosk.MPVController:
        controller = kiosk.MPVController(controller_config())
        controller._ipc = ipc
        controller._ipc_socket = False
        controller._proc = proc
        controller._generation = 4
        controller._current_log_file = "/tmp/kiosky-test/mpv-g004.log"
        return controller

    def run_send_in_thread(self, controller: kiosk.MPVController):
        result = []
        errors = []

        def send_command() -> None:
            try:
                result.append(
                    controller._send(
                        {"command": ["loadfile", "/tmp/media.mp4", "replace"]},
                        command_name="loadfile",
                    )
                )
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=send_command)
        thread.start()
        return thread, result, errors

    def test_stop_waits_for_ipc_command_before_closing_ipc(self) -> None:
        ipc = BlockingIPC()
        controller = self.build_controller(ipc)
        send_thread, result, errors = self.run_send_in_thread(controller)
        self.assertTrue(ipc.write_entered.wait(timeout=1.0))

        stop_started = threading.Event()
        stop_done = threading.Event()

        def stop_controller() -> None:
            stop_started.set()
            controller.stop()
            stop_done.set()

        stop_thread = threading.Thread(target=stop_controller)
        stop_thread.start()
        self.assertTrue(stop_started.wait(timeout=1.0))
        self.assertFalse(stop_done.wait(timeout=0.05))
        self.assertFalse(ipc.close_called.is_set())

        ipc.release_write.set()
        send_thread.join(timeout=1.0)
        stop_thread.join(timeout=1.0)

        self.assertFalse(send_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(result, [True])
        self.assertTrue(ipc.close_called.is_set())
        self.assertIsNone(controller._ipc)

    def test_restart_during_ipc_command_does_not_close_descriptor_mid_command(self) -> None:
        ipc = BlockingIPC()
        controller = self.build_controller(ipc)
        send_thread, result, errors = self.run_send_in_thread(controller)
        self.assertTrue(ipc.write_entered.wait(timeout=1.0))

        restart_started = threading.Event()
        restart_done = threading.Event()

        def restart_controller() -> None:
            restart_started.set()
            controller.restart(reason="ipc_unresponsive")
            restart_done.set()

        with mock.patch.object(controller, "_start_locked", return_value=True):
            with mock.patch("kiosk.time.sleep", return_value=None):
                restart_thread = threading.Thread(target=restart_controller)
                restart_thread.start()
                self.assertTrue(restart_started.wait(timeout=1.0))
                self.assertFalse(restart_done.wait(timeout=0.05))
                self.assertFalse(ipc.close_called.is_set())

                ipc.release_write.set()
                send_thread.join(timeout=1.0)
                restart_thread.join(timeout=1.0)

        self.assertFalse(send_thread.is_alive())
        self.assertFalse(restart_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(result, [True])
        self.assertFalse(ipc.bad_file_descriptor)
        self.assertTrue(ipc.close_called.is_set())

    def test_stop_closes_ipc_and_process_when_idle(self) -> None:
        ipc = ImmediateIPC()
        proc = FakeProcess()
        controller = self.build_controller(ipc, proc=proc)

        with mock.patch("kiosk.os.killpg") as killpg:
            controller.stop()

        self.assertTrue(ipc.closed)
        killpg.assert_called_once_with(proc.pid, kiosk.signal.SIGTERM)
        self.assertTrue(proc.wait_called)
        self.assertIsNone(controller._ipc)
        self.assertIsNone(controller._proc)

    def test_restart_closes_ipc_and_process_then_starts_when_idle(self) -> None:
        ipc = ImmediateIPC()
        proc = FakeProcess()
        controller = self.build_controller(ipc, proc=proc)

        with mock.patch("kiosk.os.killpg") as killpg:
            with mock.patch("kiosk.time.sleep", return_value=None):
                with mock.patch.object(controller, "_start_locked", return_value=True) as start:
                    controller.restart(reason="ipc_unresponsive")

        self.assertTrue(ipc.closed)
        killpg.assert_called_once_with(proc.pid, kiosk.signal.SIGTERM)
        self.assertTrue(proc.wait_called)
        self.assertIsNone(controller._ipc)
        self.assertIsNone(controller._proc)
        start.assert_called_once_with()

    def test_start_records_last_start_timestamp_after_ipc_ready(self) -> None:
        controller = kiosk.MPVController(controller_config())
        proc = FakeProcess()

        with mock.patch("kiosk.subprocess.Popen", return_value=proc):
            with mock.patch.object(controller, "_open_ipc", return_value=True):
                with mock.patch("kiosk.time.monotonic", return_value=321.5):
                    controller.start()

        self.assertEqual(controller.last_start_monotonic(), 321.5)

    def test_load_file_records_last_loadfile_attempt_timestamp(self) -> None:
        ipc = ImmediateIPC()
        controller = self.build_controller(ipc)

        with mock.patch("kiosk.time.monotonic", side_effect=[654.25, 654.30]):
            self.assertTrue(controller.load_file("/tmp/media.mp4", alias="media-test"))

        self.assertEqual(controller.last_loadfile_monotonic(), 654.25)
        self.assertEqual(ipc.write_calls, 1)

    def test_default_query_config_keeps_persistent_ping_path(self) -> None:
        ipc = ImmediateIPC()
        controller = self.build_controller(ipc)
        controller._ipc_socket = True

        with mock.patch.object(controller, "_fresh_ipc_get_property") as fresh:
            with mock.patch.object(controller, "_send", return_value={"error": "success"}) as send:
                self.assertTrue(controller.ping())

        fresh.assert_not_called()
        send.assert_called_once()

    def test_default_query_config_keeps_persistent_get_property_path(self) -> None:
        ipc = ImmediateIPC()
        controller = self.build_controller(ipc)

        with mock.patch.object(controller, "_fresh_ipc_get_property") as fresh:
            with mock.patch.object(
                controller,
                "_send",
                return_value={"error": "success", "data": "value"},
            ) as send:
                self.assertEqual(controller.get_property("playlist-count"), "value")

        fresh.assert_not_called()
        send.assert_called_once()

    def test_fresh_query_config_makes_ping_use_fresh_ipc(self) -> None:
        cfg = controller_config()
        cfg["mpv_query_uses_fresh_ipc"] = True
        controller = kiosk.MPVController(cfg)

        with mock.patch.object(
            controller,
            "_fresh_ipc_get_property",
            return_value={"error": "success", "data": False},
        ) as fresh:
            with mock.patch.object(controller, "_send") as send:
                self.assertTrue(controller.ping())

        fresh.assert_called_once_with("idle-active", timeout=controller._ipc_timeout())
        send.assert_not_called()

    def test_fresh_query_config_makes_get_property_use_fresh_ipc(self) -> None:
        cfg = controller_config()
        cfg["mpv_query_uses_fresh_ipc"] = True
        controller = kiosk.MPVController(cfg)

        with mock.patch.object(
            controller,
            "_fresh_ipc_get_property",
            return_value={"error": "success", "data": 3},
        ) as fresh:
            with mock.patch.object(controller, "_send") as send:
                self.assertEqual(controller.get_property("playlist-count", timeout=0.3), 3)

        fresh.assert_called_once_with("playlist-count", timeout=0.3)
        send.assert_not_called()

    def test_fresh_ipc_query_opens_sends_reads_and_closes_short_connection(self) -> None:
        cfg = controller_config()
        cfg["mpv_query_uses_fresh_ipc"] = True
        controller = kiosk.MPVController(cfg)
        sock = ChunkedSocket([b'{"request_id": 1, "error": "success", "data": true}\n'])

        with mock.patch.object(controller, "_open_fresh_ipc", return_value=(sock, True)):
            with self.assertLogs(level="INFO") as logs:
                response = controller._fresh_ipc_get_property("idle-active", timeout=0.2)

        self.assertEqual(response, {"request_id": 1, "error": "success", "data": True})
        self.assertTrue(sock.closed)
        self.assertIn(b'"request_id": 1', sock.sent[0])
        log_text = "\n".join(logs.output)
        self.assertIn("MPV IPC fresh query begin command=get_property", log_text)
        self.assertNotIn("idle-active", log_text)

    def test_recv_response_ignores_other_request_ids_until_expected_response(self) -> None:
        controller = self.build_controller(ImmediateIPC())
        controller._ipc_socket = True
        controller._ipc = ChunkedSocket(
            [
                b'{"request_id": 99, "error": "success", "data": "wrong"}\n'
                b'{"request_id": 7, "error": "success", "data": "expected"}\n'
            ]
        )

        response = controller._recv_response(7, 0.2)

        self.assertEqual(response["data"], "expected")
        self.assertEqual(controller._recv_buffer, "")

    def test_recv_response_timeout_logs_sanitized_counters(self) -> None:
        controller = self.build_controller(ImmediateIPC())
        controller._ipc_socket = True
        controller._ipc = ChunkedSocket(
            [
                b'{"event": "secret-event-name"}\n'
                b'not-json-secret-payload\n'
            ]
        )

        with self.assertLogs(level="WARNING") as logs:
            self.assertIsNone(controller._recv_response(7, 0.2))

        log_text = "\n".join(logs.output)
        self.assertIn("MPV IPC response timeout", log_text)
        self.assertIn("lines_read=2", log_text)
        self.assertIn("events_without_request_id=1", log_text)
        self.assertIn("responses_other_request_id=0", log_text)
        self.assertIn("responses_expected_request_id=0", log_text)
        self.assertIn("invalid_json_lines=1", log_text)
        self.assertIn("buffer_before_bytes=0", log_text)
        self.assertIn("buffer_after_bytes=0", log_text)
        self.assertNotIn("secret-event-name", log_text)
        self.assertNotIn("not-json-secret-payload", log_text)


if __name__ == "__main__":
    unittest.main()
