"""
General-purpose helpers used across the server.
"""

import sys
import time
from typing import Optional, Tuple


def get_current_time_ms() -> int:
    """
    Return the current Unix epoch time in milliseconds.

    :returns: Current Unix epoch time in milliseconds.
    """
    return int(time.time() * 1000)


_STR_OPTS = {
    "--dir": "dir_path",
    "--dbfilename": "dbfilename",
    "--appendonly": "appendonly",
    "--appenddirname": "appenddirname",
    "--appendfilename": "appendfilename",
    "--appendfsync": "appendfsync",
}

_DEFAULTS: dict = {
    "port": 6379,
    "master_host": None,
    "master_port": None,
    "dir_path": "/app",
    "dbfilename": "dump.rdb",
    "appendonly": "no",
    "appenddirname": "appendonlydir",
    "appendfilename": "appendonly.aof",
    "appendfsync": "everysec",
}


def _parse_replicaof(val: str, cfg: dict) -> None:
    parts = val.split()
    if len(parts) == 2:
        cfg["master_host"] = parts[0]
        cfg["master_port"] = int(parts[1])


def parse_args() -> Tuple[int, Optional[str], Optional[int], str, str, str, str, str, str]:
    """
    Parse server command-line arguments.

    :returns: Tuple of ``(port, master_host, master_port, dir_path, dbfilename,
        appendonly, appenddirname, appendfilename, appendfsync)``.
        ``master_host`` and ``master_port`` are ``None`` when the server runs
        as a standalone master.
    """
    cfg: dict = dict(_DEFAULTS)
    it = iter(sys.argv[1:])
    for arg in it:
        val = next(it, None)
        if val is None:
            continue
        if arg == "--port":
            cfg["port"] = int(val)
        elif arg == "--replicaof":
            _parse_replicaof(val, cfg)
        elif arg in _STR_OPTS:
            cfg[_STR_OPTS[arg]] = val
    return (
        cfg["port"], cfg["master_host"], cfg["master_port"],
        cfg["dir_path"], cfg["dbfilename"], cfg["appendonly"],
        cfg["appenddirname"], cfg["appendfilename"], cfg["appendfsync"]
    )
