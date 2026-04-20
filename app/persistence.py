"""
Append-only file (AOF) persistence: write and replay commands.
"""

import os
from typing import List

from . import state
from .commands import execute_command
from .protocol import try_parse_resp_command


def append_to_aof(parts: List[str]) -> None:
    """
    Append a command to the AOF file in RESP format.

    Does nothing when AOF is not enabled (``state.aof_file_path`` is ``None``).

    :param parts: Parsed command tokens to persist.
    """
    if state.aof_file_path is None:
        return
    resp_parts = [f"*{len(parts)}\r\n"]
    for part in parts:
        resp_parts.append(f"${len(part)}\r\n{part}\r\n")
    data = "".join(resp_parts).encode("utf-8")
    with open(state.aof_file_path, "ab") as f:
        f.write(data)
        if state.appendfsync.lower() == "always":
            f.flush()
            os.fsync(f.fileno())


def replay_aof(path: str) -> None:
    """
    Re-execute all commands stored in the AOF file to restore state on startup.

    :param path: Filesystem path to the AOF file.
    """
    try:
        with open(path, "rb") as f:
            raw = f.read().decode("utf-8", errors="replace")
    except OSError:
        return
    offset = 0
    while offset < len(raw):
        cmd_parts, consumed = try_parse_resp_command(raw[offset:])
        if consumed == 0:
            break
        offset += consumed
        if cmd_parts:
            execute_command(cmd_parts)
