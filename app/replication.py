"""
Master-replica replication: handshake, command propagation, and forwarding.
"""

import base64
import socket
from typing import List, Optional

from . import state
from .models import StoredValue
from .protocol import try_parse_resp_command
from .utils import get_current_time_ms

_EMPTY_RDB_B64 = "UkVESVMwMDA5/2NhMOXkSGD0"


def propagate_to_replicas(command: str) -> None:
    """
    Forward a raw RESP command string to all connected replicas.

    Disconnected replicas are pruned from the registry. The master offset is
    incremented only when at least one replica successfully receives the command.

    :param command: RESP-encoded command string to broadcast.
    """
    command_bytes = command.encode("utf-8")
    with state.replica_connections_lock:
        disconnected = []
        for replica in state.replica_connections:
            try:
                replica.send(command_bytes)
            except (socket.error, BrokenPipeError, OSError):
                disconnected.append(replica)

        for replica in disconnected:
            state.replica_connections.remove(replica)
            state.replica_ack_offsets.pop(replica, None)

        if state.replica_connections:
            state.master_offset += len(command_bytes)


def request_replica_acks(replicas: List[socket.socket]) -> None:
    """
    Send ``REPLCONF GETACK *`` to each replica to request an offset acknowledgement.

    :param replicas: List of replica socket connections to poll.
    """
    getack = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
    with state.replica_connections_lock:
        disconnected = []
        for replica in replicas:
            try:
                replica.send(getack)
            except (socket.error, BrokenPipeError, OSError):
                disconnected.append(replica)

        for replica in disconnected:
            if replica in state.replica_connections:
                state.replica_connections.remove(replica)
            state.replica_ack_offsets.pop(replica, None)


def build_fullresync_payload() -> bytes:
    """
    Build the ``+FULLRESYNC`` line followed by an empty RDB file payload.

    :returns: Byte string containing the ``+FULLRESYNC`` response and empty RDB dump.
    """
    fullresync = f"+FULLRESYNC {state.REPLICATION_ID} {state.REPLICATION_OFFSET}\r\n"
    rdb_data = base64.b64decode(_EMPTY_RDB_B64)
    rdb_header = f"${len(rdb_data)}\r\n"
    return fullresync.encode("utf-8") + rdb_header.encode("utf-8") + rdb_data


def process_replicated_command(
    parts: List[str], stream: socket.socket, command_length: int
) -> None:
    """
    Apply a single command received from the master to the local data store.

    Responds to ``REPLCONF GETACK`` with the current replica offset before
    updating it.

    :param parts: Parsed RESP command tokens.
    :param stream: Socket connection to the master, used for ``GETACK`` replies.
    :param command_length: Byte length of the raw command, used to advance the replica offset.
    """
    if not parts:
        return

    command = parts[0].upper()
    offset_before = state.replica_offset

    if command == "REPLCONF" and len(parts) >= 3 and parts[1].upper() == "GETACK":
        offset_str = str(offset_before)
        ack = (
            f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n"
            f"${len(offset_str)}\r\n{offset_str}\r\n"
        )
        stream.send(ack.encode("utf-8"))

    if command == "SET" and len(parts) >= 3:
        key, value = parts[1], parts[2]
        expiry_ms: Optional[int] = None
        i = 3
        while i < len(parts) - 1:
            opt = parts[i].upper()
            if opt == "PX":
                expiry_ms = get_current_time_ms() + int(parts[i + 1])
                break
            elif opt == "EX":
                expiry_ms = get_current_time_ms() + int(parts[i + 1]) * 1000
                break
            i += 1
        with state.data_store_lock:
            state.data_store[key] = StoredValue(value=value, expiry_ms=expiry_ms)

    state.replica_offset += command_length


def process_buffered_commands(command_buffer: List[str], stream: socket.socket) -> None:
    """
    Drain all complete RESP commands from the buffer and apply each one.

    :param command_buffer: Mutable list of partial/full RESP data chunks.
    :param stream: Master socket passed through to :func:`process_replicated_command`.
    """
    buffered = "".join(command_buffer)
    processed = 0

    while True:
        remaining = buffered[processed:]
        if not remaining:
            break
        parts, length = try_parse_resp_command(remaining)
        if parts is None or length == 0:
            break
        process_replicated_command(parts, stream, length)
        processed += length

    if processed:
        command_buffer.clear()
        command_buffer.append(buffered[processed:])


def connect_to_master(host: str, master_port: int, replica_port: int) -> None:
    """
    Connect to the master, complete the PSYNC handshake, and stream commands.

    Runs in a background daemon thread for the lifetime of the replica process.

    :param host: Hostname or IP address of the master.
    :param master_port: TCP port the master is listening on.
    :param replica_port: This replica's own listening port, announced during handshake.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, master_port))

        port_str = str(replica_port)
        for cmd in [
            b"*1\r\n$4\r\nPING\r\n",
            (
                f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n"
                f"${len(port_str)}\r\n{port_str}\r\n"
            ).encode(),
            b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
        ]:
            sock.send(cmd)
            sock.recv(4096)

        sock.send(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        buf = sock.recv(4096)
        text = buf.decode("utf-8", errors="ignore")

        end = text.find("\r\n")
        if end == -1:
            return

        rdb_start = end + 2
        while rdb_start >= len(text) or text[rdb_start] != "$":
            chunk = sock.recv(4096)
            if not chunk:
                return
            buf += chunk
            text = buf.decode("utf-8", errors="ignore")

        rdb_len_end = text.find("\r\n", rdb_start)
        while rdb_len_end == -1:
            chunk = sock.recv(4096)
            if not chunk:
                return
            buf += chunk
            text = buf.decode("utf-8", errors="ignore")
            rdb_len_end = text.find("\r\n", rdb_start)

        try:
            rdb_length = int(text[rdb_start + 1 : rdb_len_end])
        except ValueError:
            return

        rdb_data_start = len(text[:rdb_len_end].encode("utf-8")) + 2
        rdb_data_end = rdb_data_start + rdb_length

        while len(buf) < rdb_data_end:
            chunk = sock.recv(4096)
            if not chunk:
                return
            buf += chunk

        command_buffer: List[str] = []
        if len(buf) > rdb_data_end:
            command_buffer.append(buf[rdb_data_end:].decode("utf-8", errors="ignore"))

        if command_buffer and command_buffer[0]:
            process_buffered_commands(command_buffer, sock)

        while True:
            data = sock.recv(4096)
            if not data:
                break
            command_buffer.append(data.decode("utf-8", errors="ignore"))
            process_buffered_commands(command_buffer, sock)

    except (socket.error, ConnectionRefusedError, TimeoutError, OSError):
        pass
