"""
Client connection handler and main server loop.
"""

import hashlib
import os
import socket
import threading
import time
from typing import List

from . import state
from .commands import execute_command
from .lists import cmd_rpush, cmd_lpush, cmd_lrange, cmd_llen, cmd_lpop, cmd_blpop
from .persistence import append_to_aof, replay_aof
from .protocol import parse_resp_array
from .pubsub import cmd_publish, cmd_subscribe, cmd_unsubscribe
from .rdb import load_rdb_file
from .replication import (
    build_fullresync_payload,
    connect_to_master,
    propagate_to_replicas,
    request_replica_acks,
)
from .streams import cmd_xadd, cmd_xread, cmd_xrange
from .utils import get_current_time_ms, parse_args

_WRONGTYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


def _notify_key_modified(key: str, modifier: socket.socket) -> None:
    """
    Mark all clients watching *key* (other than *modifier*) as dirty so that
    their pending EXEC will be aborted.

    :param key: The key that was just written.
    :param modifier: The client performing the write (not marked dirty).
    """
    with state.watch_lock:
        watchers = state.key_watchers.get(key)
        if watchers:
            for watcher in watchers:
                if watcher is not modifier:
                    state.watch_dirty[watcher] = True


def _clear_watch_state(client: socket.socket, watched_keys: set) -> None:
    """
    Remove *client* from all key-watcher sets and clear its dirty flag.

    :param client: The client whose watch state should be removed.
    :param watched_keys: The set of keys this client is currently watching
        (will be cleared in-place).
    """
    with state.watch_lock:
        for key in watched_keys:
            watchers = state.key_watchers.get(key)
            if watchers:
                watchers.discard(client)
                if not watchers:
                    del state.key_watchers[key]
        state.watch_dirty.pop(client, None)
    watched_keys.clear()


def handle_client(client: socket.socket) -> None:
    """
    Manage the full lifecycle of a single client connection.

    Maintains per-connection state for transactions, authentication, pub/sub
    subscriptions, and whether the connection has been promoted to a replica
    replication stream.

    :param client: Connected client socket.
    """
    in_transaction = False
    transaction_queue: List[List[str]] = []
    watched_keys: set = set()
    is_replication_connection = False
    is_subscribed_mode = False
    is_authenticated = "nopass" in state.default_user_flags

    while True:
        try:
            buf = client.recv(1024)
            if not buf:
                break

            input_str = buf.decode("utf-8")
            parts = parse_resp_array(input_str)
            if not parts:
                continue

            command = parts[0].upper()
            response = ""

            if is_subscribed_mode:
                allowed = {
                    "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE",
                    "PUNSUBSCRIBE", "PING", "QUIT", "RESET",
                }
                if command not in allowed:
                    msg = command.lower()
                    client.send(
                        f"-ERR Can't execute '{msg}': only (P|S)SUBSCRIBE / "
                        f"(P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed "
                        f"in this context\r\n".encode("utf-8")
                    )
                    continue

            if not is_authenticated and command not in {"AUTH", "HELLO", "QUIT", "RESET"}:
                client.send(b"-NOAUTH Authentication required.\r\n")
                continue

            if command == "MULTI":
                in_transaction = True
                transaction_queue = []
                response = "+OK\r\n"

            elif command == "EXEC":
                if in_transaction:
                    if state.watch_dirty.get(client, False):
                        response = "*-1\r\n"
                    else:
                        _write_cmds = {"SET", "INCR", "ZADD", "ZREM", "GEOADD", "XADD", "RPUSH", "LPUSH"}
                        responses = []
                        for cmd in transaction_queue:
                            r = execute_command(cmd)
                            responses.append(r)
                            if cmd and len(cmd) >= 2 and cmd[0].upper() in _write_cmds:
                                _notify_key_modified(cmd[1], client)
                        response = f"*{len(responses)}\r\n" + "".join(responses)
                    in_transaction = False
                    transaction_queue = []
                    _clear_watch_state(client, watched_keys)
                else:
                    response = "-ERR EXEC without MULTI\r\n"

            elif command == "DISCARD":
                if in_transaction:
                    in_transaction = False
                    transaction_queue = []
                    _clear_watch_state(client, watched_keys)
                    response = "+OK\r\n"
                else:
                    response = "-ERR DISCARD without MULTI\r\n"

            elif command == "WATCH" and len(parts) >= 2:
                if in_transaction:
                    response = "-ERR WATCH inside MULTI is not allowed\r\n"
                else:
                    with state.watch_lock:
                        for key in parts[1:]:
                            state.key_watchers.setdefault(key, set()).add(client)
                            watched_keys.add(key)
                    response = "+OK\r\n"

            elif command == "UNWATCH":
                _clear_watch_state(client, watched_keys)
                response = "+OK\r\n"

            elif in_transaction:
                transaction_queue.append(parts)
                response = "+QUEUED\r\n"

            elif command == "PING" and is_subscribed_mode:
                response = "*2\r\n$4\r\npong\r\n$0\r\n\r\n"

            elif command == "AUTH" and len(parts) >= 3:
                username, password = parts[1], parts[2]
                if username == "default":
                    if "nopass" in state.default_user_flags:
                        is_authenticated = True
                        response = "+OK\r\n"
                    else:
                        pw_hash = hashlib.sha256(password.encode()).hexdigest()
                        if pw_hash in state.default_user_passwords:
                            is_authenticated = True
                            response = "+OK\r\n"
                        else:
                            response = "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
                else:
                    response = "-WRONGPASS invalid username-password pair or user is disabled.\r\n"

            elif command == "INFO":
                is_replica = state.master_host is not None
                role = "slave" if is_replica else "master"
                if len(parts) == 1 or parts[1].upper() == "REPLICATION":
                    if is_replica:
                        info = f"role:{role}"
                    else:
                        info = (
                            f"role:{role}\r\n"
                            f"master_replid:{state.REPLICATION_ID}\r\n"
                            f"master_repl_offset:{state.REPLICATION_OFFSET}"
                        )
                    response = f"${len(info)}\r\n{info}\r\n"
                else:
                    response = "$0\r\n\r\n"

            elif command == "CONFIG" and len(parts) >= 3 and parts[1].upper() == "GET":
                parameter = parts[2].lower()
                value = None
                if parameter == "dir":
                    value = state.dir_path
                elif parameter == "dbfilename":
                    value = state.dbfilename
                elif parameter == "appendonly":
                    value = state.appendonly
                elif parameter == "appenddirname":
                    value = state.appenddirname
                elif parameter == "appendfilename":
                    value = state.appendfilename
                elif parameter == "appendfsync":
                    value = state.appendfsync
                if value is not None:
                    response = (
                        f"*2\r\n${len(parameter)}\r\n{parameter}\r\n"
                        f"${len(value)}\r\n{value}\r\n"
                    )
                else:
                    response = "*0\r\n"

            elif command == "KEYS" and len(parts) >= 2:
                pattern = parts[1]
                with state.data_store_lock:
                    matching = [
                        k for k, v in state.data_store.items()
                        if (pattern == "*" or k == pattern)
                        and (not v.expiry_ms or get_current_time_ms() <= v.expiry_ms)
                    ]
                response = f"*{len(matching)}\r\n"
                for k in matching:
                    response += f"${len(k)}\r\n{k}\r\n"

            elif command == "REPLCONF":
                if len(parts) >= 3 and parts[1].upper() == "ACK":
                    try:
                        ack_offset = int(parts[2])
                        with state.replica_connections_lock:
                            if client in state.replica_connections:
                                state.replica_ack_offsets[client] = ack_offset
                    except ValueError:
                        pass
                    response = ""
                else:
                    response = "+OK\r\n"

            elif command == "PSYNC" and len(parts) >= 3:
                client.send(build_fullresync_payload())
                with state.replica_connections_lock:
                    state.replica_connections.append(client)
                    state.replica_ack_offsets[client] = 0
                is_replication_connection = True
                continue

            elif command == "SET" and len(parts) >= 3:
                response = execute_command(parts)
                if not is_replication_connection:
                    propagate_to_replicas(input_str)
                    _notify_key_modified(parts[1], client)
                    if state.appendonly.lower() == "yes":
                        append_to_aof(parts)

            elif command in {
                "PING", "ECHO", "GET", "INCR",
                "ACL",
                "ZADD", "ZRANK", "ZRANGE", "ZCARD", "ZSCORE", "ZREM",
                "GEOADD", "GEOPOS", "GEODIST", "GEOSEARCH",
            }:
                response = execute_command(parts)
                if command in {"INCR", "ZADD", "ZREM", "GEOADD"} and len(parts) >= 2:
                    _notify_key_modified(parts[1], client)

            elif command == "RPUSH" and len(parts) >= 3:
                response = cmd_rpush(parts)
                _notify_key_modified(parts[1], client)

            elif command == "LPUSH" and len(parts) >= 3:
                response = cmd_lpush(parts)
                _notify_key_modified(parts[1], client)

            elif command == "LRANGE" and len(parts) >= 4:
                response = cmd_lrange(parts)

            elif command == "LLEN" and len(parts) >= 2:
                response = cmd_llen(parts)

            elif command == "LPOP" and len(parts) >= 2:
                result = cmd_lpop(parts)
                if result is None:
                    continue
                response = result

            elif command == "BLPOP" and len(parts) >= 3:
                response = cmd_blpop(client, parts)

            # ------------------------------------------------------------------
            # Key metadata
            # ------------------------------------------------------------------
            elif command == "TYPE" and len(parts) >= 2:
                response = _cmd_type(parts)

            # ------------------------------------------------------------------
            # Replication wait
            # ------------------------------------------------------------------
            elif command == "WAIT" and len(parts) >= 3:
                response = _cmd_wait(parts)

            # ------------------------------------------------------------------
            # Pub / sub
            # ------------------------------------------------------------------
            elif command == "PUBLISH" and len(parts) >= 3:
                response = cmd_publish(parts)

            elif command == "SUBSCRIBE" and len(parts) >= 2:
                cmd_subscribe(client, parts)
                is_subscribed_mode = True
                response = ""

            elif command == "UNSUBSCRIBE" and len(parts) >= 2:
                is_subscribed_mode = cmd_unsubscribe(client, parts)
                response = ""

            # ------------------------------------------------------------------
            # Stream commands
            # ------------------------------------------------------------------
            elif command == "XADD" and len(parts) >= 4:
                response = cmd_xadd(parts)
                if not response.startswith("-"):
                    _notify_key_modified(parts[1], client)

            elif command == "XREAD" and len(parts) >= 4:
                response = cmd_xread(client, parts)

            elif command == "XRANGE" and len(parts) >= 4:
                response = cmd_xrange(parts)

            else:
                response = "-ERR unknown command\r\n"

            if response and not is_replication_connection:
                client.send(response.encode("utf-8"))

        except (socket.error, BrokenPipeError, ConnectionResetError, OSError):
            break

    # Cleanup replica state
    with state.replica_connections_lock:
        if client in state.replica_connections:
            state.replica_connections.remove(client)
        state.replica_ack_offsets.pop(client, None)

    # Cleanup subscription state
    with state.subscriptions_lock:
        if client in state.client_subscriptions:
            for channel in state.client_subscriptions[client]:
                if channel in state.channel_subscribers:
                    state.channel_subscribers[channel].discard(client)
                    if not state.channel_subscribers[channel]:
                        del state.channel_subscribers[channel]
            del state.client_subscriptions[client]

    # Cleanup watch state
    _clear_watch_state(client, watched_keys)

    client.close()


# ---------------------------------------------------------------------------
# Type command
# ---------------------------------------------------------------------------


def _cmd_type(parts: List[str]) -> str:
    """
    Handle the TYPE command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded type string (``+string``, ``+list``, ``+stream``, ``+zset``, or ``+none``).
    """
    key = parts[1]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            return "+none\r\n"
        if stored.value is not None:
            if stored.expiry_ms and get_current_time_ms() > stored.expiry_ms:
                del state.data_store[key]
                return "+none\r\n"
            return "+string\r\n"
        if stored.list_value is not None:
            return "+list\r\n"
        if stored.stream is not None:
            return "+stream\r\n"
        if stored.sorted_set is not None:
            return "+zset\r\n"
        return "+none\r\n"


# ---------------------------------------------------------------------------
# WAIT command
# ---------------------------------------------------------------------------


def _cmd_wait(parts: List[str]) -> str:
    """
    Handle the WAIT command, blocking until replicas acknowledge.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded integer count of replicas that acknowledged.
    """
    try:
        num_replicas = int(parts[1])
        timeout_ms = int(parts[2])
    except ValueError:
        return "-ERR value is not an integer or out of range\r\n"

    with state.replica_connections_lock:
        replicas = list(state.replica_connections)
        current_offset = state.master_offset

    if not replicas:
        return ":0\r\n"
    if current_offset == 0:
        return f":{len(replicas)}\r\n"

    request_replica_acks(replicas)
    deadline = time.time() + timeout_ms / 1000.0
    acked = 0

    while True:
        with state.replica_connections_lock:
            acked = sum(
                1 for r in replicas
                if r in state.replica_ack_offsets
                and state.replica_ack_offsets[r] >= current_offset
            )
        if acked >= num_replicas or time.time() >= deadline:
            break
        time.sleep(0.01)

    return f":{acked}\r\n"


# ---------------------------------------------------------------------------
# Server entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """
    Start the Redis server, load persisted data, and accept client connections.
    """

    port, master_host, master_port, dir_path, dbfilename, appendonly, appenddirname, appendfilename, appendfsync = parse_args()

    state.dir_path = dir_path
    state.dbfilename = dbfilename
    state.master_host = master_host
    state.appendonly = appendonly
    state.appenddirname = appenddirname
    state.appendfilename = appendfilename
    state.appendfsync = appendfsync

    load_rdb_file(os.path.join(dir_path, dbfilename))

    if appendonly.lower() == "yes":
        aof_dir = os.path.join(dir_path, appenddirname)
        os.makedirs(aof_dir, exist_ok=True)
        incr_aof = os.path.join(aof_dir, f"{appendfilename}.1.incr.aof")
        manifest_path = os.path.join(aof_dir, f"{appendfilename}.manifest")
        state.aof_file_path = incr_aof
        if os.path.exists(manifest_path):
            # Parse manifest and replay each listed file in order
            with open(manifest_path, "r") as mf:
                for line in mf:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split()
                    # Format: file <filename> seq <n> type <t>
                    if len(parts) >= 2 and parts[0] == "file":
                        entry_path = os.path.join(aof_dir, parts[1])
                        if os.path.exists(entry_path):
                            replay_aof(entry_path)
        else:
            if not os.path.exists(incr_aof):
                open(incr_aof, "ab").close()
            with open(manifest_path, "w") as mf:
                mf.write(f"file {appendfilename}.1.incr.aof seq 1 type i\n")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", port))
    server_socket.listen(5)

    if master_host is not None and master_port is not None:
        threading.Thread(
            target=connect_to_master,
            args=(master_host, master_port, port),
            daemon=True,
        ).start()

    while True:
        client, _ = server_socket.accept()
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        threading.Thread(target=handle_client, args=(
            client,), daemon=True).start()
