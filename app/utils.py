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


def parse_args() -> Tuple[int, Optional[str], Optional[int], str, str]:
    """
    Parse server command-line arguments.

    :returns: Tuple of ``(port, master_host, master_port, dir_path, dbfilename)``.
        ``master_host`` and ``master_port`` are ``None`` when the server runs
        as a standalone master.
    """
    port = 6379
    master_host: Optional[str] = None
    master_port: Optional[int] = None
    dir_path = "/tmp/redis-files"
    dbfilename = "dump.rdb"

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "--port" and i + 1 < len(sys.argv):
            port = int(sys.argv[i + 1])
            i += 2
        elif arg == "--replicaof" and i + 1 < len(sys.argv):
            parts = sys.argv[i + 1].split()
            if len(parts) == 2:
                master_host = parts[0]
                master_port = int(parts[1])
            i += 2
        elif arg == "--dir" and i + 1 < len(sys.argv):
            dir_path = sys.argv[i + 1]
            i += 2
        elif arg == "--dbfilename" and i + 1 < len(sys.argv):
            dbfilename = sys.argv[i + 1]
            i += 2
        else:
            i += 1

    return port, master_host, master_port, dir_path, dbfilename
