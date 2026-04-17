from __future__ import annotations

import errno
import os
import pty
import re
import select
import shutil
import subprocess
import sys
from typing import Any, Callable, Optional


SEVEN_ZIP_BIN = "7z"
SEVEN_ZIP_PROGRESS_RE = re.compile(r"(?<!\d)(100|[1-9]?\d)%")
ZIP_MIN_COMPRESSION_SAVINGS_RATIO = 0.01


def ensure_7z_available() -> None:
    if shutil.which(SEVEN_ZIP_BIN) is None:
        raise RuntimeError("7z binary is required but not found in PATH")


def _resolve_tools_export(name: str, fallback: Callable[..., Any]) -> Callable[..., Any]:
    tools_module = sys.modules.get("open_workshop_storage.utils")
    if tools_module is None:
        return fallback
    override = getattr(tools_module, name, fallback)
    if callable(override):
        return override
    return fallback


def _run_7z(args: list[str], cwd: Optional[str] = None) -> subprocess.CompletedProcess:
    _resolve_tools_export("ensure_7z_available", ensure_7z_available)()
    return subprocess.run(
        [SEVEN_ZIP_BIN, *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )


def _drain_7z_progress_stream(
    chunk: str,
    state: dict[str, Any],
    on_progress: Optional[Callable[[int], None]] = None,
) -> None:
    pending = str(state.get("pending", "")) + chunk
    if len(pending) > 256:
        pending = pending[-256:]
    last_percent = int(state.get("last_percent", -1))
    for match in SEVEN_ZIP_PROGRESS_RE.finditer(pending):
        percent = int(match.group(1))
        if percent <= last_percent:
            continue
        last_percent = percent
        if on_progress is not None:
            on_progress(percent)
    state["pending"] = pending
    state["last_percent"] = last_percent


def _run_7z_with_progress(
    args: list[str],
    cwd: Optional[str] = None,
    on_progress: Optional[Callable[[int], None]] = None,
) -> subprocess.CompletedProcess:
    _resolve_tools_export("ensure_7z_available", ensure_7z_available)()
    master_fd, slave_fd = pty.openpty()
    process = subprocess.Popen(
        [SEVEN_ZIP_BIN, *args],
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=slave_fd,
        stderr=slave_fd,
        close_fds=True,
    )
    os.close(slave_fd)

    output_chunks: list[str] = []
    progress_state = {"pending": "", "last_percent": -1}
    try:
        while True:
            ready, _, _ = select.select([master_fd], [], [], 0.1)
            if master_fd in ready:
                try:
                    data = os.read(master_fd, 4096)
                except OSError as exc:
                    if exc.errno == errno.EIO:
                        break
                    raise
                if not data:
                    break
                chunk = data.decode("utf-8", errors="replace")
                output_chunks.append(chunk)
                _drain_7z_progress_stream(chunk, progress_state, on_progress)
            if process.poll() is not None and master_fd not in ready:
                try:
                    data = os.read(master_fd, 4096)
                except OSError as exc:
                    if exc.errno == errno.EIO:
                        break
                    raise
                if not data:
                    break
                chunk = data.decode("utf-8", errors="replace")
                output_chunks.append(chunk)
                _drain_7z_progress_stream(chunk, progress_state, on_progress)
    finally:
        os.close(master_fd)

    returncode = process.wait()
    output = "".join(output_chunks)

    return subprocess.CompletedProcess(
        process.args,
        returncode,
        stdout=output,
        stderr="",
    )


def _run_7z_list(
    path: str,
    archive_type: Optional[str] = None,
) -> tuple[Optional[list[dict[str, str]]], str, int]:
    args = ["l", "-slt"]
    if archive_type:
        args.append(f"-t{archive_type}")
    args.append(path)
    result = _run_7z(args)
    if result.returncode != 0:
        output = (result.stderr or "") + "\n" + (result.stdout or "")
        return None, output.strip(), result.returncode
    entries: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            if current:
                entries.append(current)
                current = {}
            continue
        if " = " in line:
            key, value = line.split(" = ", 1)
            current[key] = value
    if current:
        entries.append(current)
    return entries, "", 0


def _list_7z_entries(path: str, archive_type: Optional[str] = None) -> Optional[list[dict[str, str]]]:
    entries, _, code = _run_7z_list(path, archive_type=archive_type)
    if code != 0:
        return None
    return entries


def probe_archive(path: str) -> tuple[Optional[str], bool, Optional[list[dict[str, str]]]]:
    entries, error, code = _run_7z_list(path)
    if code != 0 or not entries:
        lowered = error.lower()
        if "password" in lowered or "encrypted" in lowered:
            return None, True, None
        return None, False, None
    archive_type = entries[0].get("Type")
    archive_type = archive_type.lower() if archive_type else None
    encrypted = False
    for entry in entries:
        if entry.get("Encrypted") == "+":
            encrypted = True
            break
    return archive_type, encrypted, entries


def archive_entries_unpacked_bytes(entries: Optional[list[dict[str, str]]]) -> Optional[int]:
    if entries is None:
        return None

    total = 0
    for entry in entries:
        if entry.get("Type"):
            continue
        if not entry.get("Path"):
            continue
        if entry.get("Folder") == "+":
            continue
        raw_size = entry.get("Size")
        if raw_size is None:
            return None
        try:
            size = int(raw_size)
        except (TypeError, ValueError):
            return None
        if size < 0:
            return None
        total += size
    return total


def archive_entries_packed_bytes(entries: Optional[list[dict[str, str]]]) -> Optional[int]:
    if entries is None:
        return None

    total = 0
    for entry in entries:
        if entry.get("Type"):
            continue
        if not entry.get("Path"):
            continue
        if entry.get("Folder") == "+":
            continue
        raw_size = entry.get("Packed Size")
        if raw_size is None:
            return None
        try:
            size = int(raw_size)
        except (TypeError, ValueError):
            return None
        if size < 0:
            return None
        total += size
    return total


def zip_dir_with_level(
    src_dir: str,
    dest_zip_path: str,
    compresslevel: int = 3,
    on_progress: Optional[Callable[[int], None]] = None,
) -> None:
    src_dir = os.path.abspath(src_dir)
    os.makedirs(os.path.dirname(dest_zip_path), exist_ok=True)
    if os.path.exists(dest_zip_path):
        os.remove(dest_zip_path)
    result = _run_7z_with_progress(
        [
            "a",
            "-tzip",
            "-mm=Deflate",
            f"-mx={compresslevel}",
            "-mmt=on",
            "-bb0",
            "-bso0",
            "-bsp1",
            dest_zip_path,
            ".",
        ],
        cwd=src_dir,
        on_progress=on_progress,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout).strip() or "7z failed to create zip")


def zip_uses_deflated_or_better(
    path: str,
    entries: Optional[list[dict[str, str]]] = None,
) -> bool:
    entries = entries or _list_7z_entries(path, archive_type="zip")
    if not entries:
        return False

    for entry in entries:
        if entry.get("Type"):
            continue
        if not entry.get("Path"):
            continue
        if entry.get("Folder") == "+":
            continue
        if entry.get("Encrypted") == "+":
            return False
        method = (entry.get("Method") or "").lower()
        if not method:
            return False
        if "deflate" in method:
            continue
        if "lzma" in method or "bzip2" in method or "ppmd" in method:
            continue
        if "store" in method:
            continue
        return False

    unpacked_bytes = _resolve_tools_export(
        "archive_entries_unpacked_bytes",
        archive_entries_unpacked_bytes,
    )(entries)
    packed_bytes = _resolve_tools_export(
        "archive_entries_packed_bytes",
        archive_entries_packed_bytes,
    )(entries)
    if unpacked_bytes is None or packed_bytes is None:
        return False
    if unpacked_bytes <= 0:
        return True
    if packed_bytes >= unpacked_bytes:
        return False

    savings_ratio = (unpacked_bytes - packed_bytes) / unpacked_bytes
    return savings_ratio >= ZIP_MIN_COMPRESSION_SAVINGS_RATIO


def _find_single_tar(dest_dir: str) -> Optional[str]:
    entries = os.listdir(dest_dir)
    if len(entries) != 1:
        return None
    path = os.path.join(dest_dir, entries[0])
    if os.path.isfile(path) and entries[0].lower().endswith(".tar"):
        return path
    return None


def safe_extract_archive(
    archive_path: str,
    dest_dir: str,
    entries: Optional[list[dict[str, str]]] = None,
    on_progress: Optional[Callable[[int], None]] = None,
) -> None:
    dest_dir = os.path.abspath(dest_dir)
    entries = entries or _list_7z_entries(archive_path)
    if entries is None:
        raise ValueError("Invalid archive")
    for entry in entries:
        if entry.get("Encrypted") == "+":
            raise ValueError("Encrypted archive entries are not supported")
        if entry.get("Type"):
            continue
        name = (entry.get("Path") or "").replace("\\", "/")
        if not name:
            continue
        target_path = os.path.abspath(os.path.join(dest_dir, name))
        if os.path.commonpath([target_path, dest_dir]) != dest_dir:
            raise ValueError("Unsafe path in archive")
    os.makedirs(dest_dir, exist_ok=True)
    result = _run_7z_with_progress(
        [
            "x",
            f"-o{dest_dir}",
            "-y",
            "-bb0",
            "-bso0",
            "-bsp1",
            archive_path,
        ],
        on_progress=on_progress,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "7z failed to extract archive")

    archive_type = (entries[0].get("Type") or "").lower()
    if archive_type in {"gzip", "bzip2", "xz"}:
        tar_path = _find_single_tar(dest_dir)
        if tar_path:
            _resolve_tools_export("safe_extract_archive", safe_extract_archive)(
                tar_path,
                dest_dir,
                on_progress=on_progress,
            )
            os.remove(tar_path)
