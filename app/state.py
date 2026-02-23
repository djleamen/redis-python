"""
Shared mutable state for the Redis server.

Import this module and access attributes directly. Do *not* copy values into
local module globals, as mutations would not be reflected across modules.
"""

import socket
import threading
from collections import deque
from typing import Dict, List, Optional, Set

from .models import StoredValue

# ---------------------------------------------------------------------------
# Data store
# ---------------------------------------------------------------------------

data_store: Dict[str, StoredValue] = {}
data_store_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Blocking list clients
# ---------------------------------------------------------------------------

blocked_clients: Dict[str, deque] = {}
blocked_clients_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Blocking stream readers
# ---------------------------------------------------------------------------

blocked_stream_readers: Dict[str, deque] = {}
blocked_stream_readers_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Pub / sub
# ---------------------------------------------------------------------------

channel_subscribers: Dict[str, Set[socket.socket]] = {}
client_subscriptions: Dict[socket.socket, Set[str]] = {}
subscriptions_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Replication
# ---------------------------------------------------------------------------

replica_connections: List[socket.socket] = []
replica_connections_lock = threading.Lock()
replica_ack_offsets: Dict[socket.socket, int] = {}

replica_offset: int = 0
master_offset: int = 0

REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
REPLICATION_OFFSET = 0

# ---------------------------------------------------------------------------
# ACL / authentication
# ---------------------------------------------------------------------------

default_user_flags: Set[str] = {"nopass"}
default_user_passwords: List[str] = []

# ---------------------------------------------------------------------------
# Server configuration (populated at startup via parse_args)
# ---------------------------------------------------------------------------

dir_path: str = "/tmp/redis-files"
dbfilename: str = "dump.rdb"
master_host: Optional[str] = None
