"""
Client connection handler and main server loop.
"""

import hashlib
import os
import socket
import threading
import time
from dataclasses import dataclass, field
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
_WRONGPASS = "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
_WRITE_CMDS = {"SET", "INCR", "ZADD", "ZREM", "GEOADD", "XADD", "RPUSH", "LPUSH"}
_EXECUTE_CMDS = {
    "PING", "ECHO", "GET", "INCR",
    "ACL",
    "ZADD", "ZRANK", "ZRANGE", "ZCARD", "ZSCORE", "ZREM",
    "GEOADD", "GEOPOS", "GEODIST", "GEOSEARCH",
}
_NOTIFY_CMDS = {"INCR", "ZADD", "ZREM", "GEOADD"}
_SUBSCRIBED_ALLOWED = {
    "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE",
    "PUNSUBSCRIBE", "PING", "QUIT", "RESET",
}


@dataclass
class _ClientCtx:
    client: socket.socket
    in_transaction: bool = False
    transaction_queue: List[List[str]] = field(default_factory=list)
    watched_keys: set = field(default_factory=set)
    is_replication_connection: bool = False
    is_subscribed_mode: bool = False
    is_authenticated: bool = field(default_factory=lambda: "nopass" in state.default_user_flags)


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


def _handle_multi(ctx: "_ClientCtx") -> str:
    ctx.in_transaction = True
    ctx.transaction_queue = []
    return "+OK\r\n"


def _handle_exec(ctx: "_ClientCtx") -> str:
    if not ctx.in_transaction:
        return "-ERR EXEC without MULTI\r\n"
    if state.watch_dirty.get(ctx.client, False):
        response = "*-1\r\n"
    else:
        responses = []
        for cmd in ctx.transaction_queue:
            r = execute_command(cmd)
            responses.append(r)
            if cmd and len(cmd) >= 2 and cmd[0].upper() in _WRITE_CMDS:
                _notify_key_modified(cmd[1], ctx.client)
        response = f"*{len(responses)}\r\n" + "".join(responses)
    ctx.in_transaction = False
    ctx.transaction_queue = []
    _clear_watch_state(ctx.client, ctx.watched_keys)
    return response


def _handle_discard(ctx: "_ClientCtx") -> str:
    if not ctx.in_transaction:
        return "-ERR DISCARD without MULTI\r\n"
    ctx.in_transaction = False
    ctx.transaction_queue = []
    _clear_watch_state(ctx.client, ctx.watched_keys)
    return "+OK\r\n"


def _handle_watch(ctx: "_ClientCtx", parts: List[str]) -> str:
    if ctx.in_transaction:
        return "-ERR WATCH inside MULTI is not allowed\r\n"
    with state.watch_lock:
        for key in parts[1:]:
            state.key_watchers.setdefault(key, set()).add(ctx.client)
            ctx.watched_keys.add(key)
    return "+OK\r\n"


def _handle_auth(ctx: "_ClientCtx", parts: List[str]) -> str:
    username, password = parts[1], parts[2]
    if username != "default":
        return _WRONGPASS
    if "nopass" in state.default_user_flags:
        ctx.is_authenticated = True
        return "+OK\r\n"
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    if pw_hash in state.default_user_passwords:
        ctx.is_authenticated = True
        return "+OK\r\n"
    return _WRONGPASS


def _handle_info(parts: List[str]) -> str:
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
        return f"${len(info)}\r\n{info}\r\n"
    return "$0\r\n\r\n"


def _handle_config_get(parts: List[str]) -> str:
    parameter = parts[2].lower()
    _config_map = {
        "dir": state.dir_path,
        "dbfilename": state.dbfilename,
        "appendonly": state.appendonly,
        "appenddirname": state.appenddirname,
        "appendfilename": state.appendfilename,
        "appendfsync": state.appendfsync,
    }
    value = _config_map.get(parameter)
    if value is not None:
        return (
            f"*2\r\n${len(parameter)}\r\n{parameter}\r\n"
            f"${len(value)}\r\n{value}\r\n"
        )
    return "*0\r\n"


def _handle_keys(parts: List[str]) -> str:
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
    return response


def _handle_replconf(ctx: "_ClientCtx", parts: List[str]) -> str:
    if len(parts) >= 3 and parts[1].upper() == "ACK":
        try:
            ack_offset = int(parts[2])
            with state.replica_connections_lock:
                if ctx.client in state.replica_connections:
                    state.replica_ack_offsets[ctx.client] = ack_offset
        except ValueError:
            pass
        return ""
    return "+OK\r\n"


def _dispatch_command(ctx: "_ClientCtx", parts: List[str], input_str: str) -> str:
    """Route a single command to the appropriate handler; return RESP response."""
    command = parts[0].upper()

    if command == "MULTI":
        return _handle_multi(ctx)
    if command == "EXEC":
        return _handle_exec(ctx)
    if command == "DISCARD":
        return _handle_discard(ctx)
    if command == "WATCH" and len(parts) >= 2:
        return _handle_watch(ctx, parts)
    if command == "UNWATCH":
        _clear_watch_state(ctx.client, ctx.watched_keys)
        return "+OK\r\n"
    if ctx.in_transaction:
        ctx.transaction_queue.append(parts)
        return "+QUEUED\r\n"
    if command == "PING" and ctx.is_subscribed_mode:
        return "*2\r\n$4\r\npong\r\n$0\r\n\r\n"
    if command == "AUTH" and len(parts) >= 3:
        return _handle_auth(ctx, parts)
    if command == "INFO":
        return _handle_info(parts)
    if command == "CONFIG" and len(parts) >= 3 and parts[1].upper() == "GET":
        return _handle_config_get(parts)
    if command == "KEYS" and len(parts) >= 2:
        return _handle_keys(parts)
    if command == "REPLCONF":
        return _handle_replconf(ctx, parts)
    if command == "PSYNC" and len(parts) >= 3:
        ctx.client.send(build_fullresync_payload())
        with state.replica_connections_lock:
            state.replica_connections.append(ctx.client)
            state.replica_ack_offsets[ctx.client] = 0
        ctx.is_replication_connection = True
        return ""
    if command == "SET" and len(parts) >= 3:
        response = execute_command(parts)
        if not ctx.is_replication_connection:
            propagate_to_replicas(input_str)
            _notify_key_modified(parts[1], ctx.client)
            if state.appendonly.lower() == "yes":
                append_to_aof(parts)
        return response
    if command in _EXECUTE_CMDS:
        response = execute_command(parts)
        if command in _NOTIFY_CMDS and len(parts) >= 2:
            _notify_key_modified(parts[1], ctx.client)
        return response
    if command == "RPUSH" and len(parts) >= 3:
        _notify_key_modified(parts[1], ctx.client)
        return cmd_rpush(parts)
    if command == "LPUSH" and len(parts) >= 3:
        _notify_key_modified(parts[1], ctx.client)
        return cmd_lpush(parts)
    if command == "LRANGE" and len(parts) >= 4:
        return cmd_lrange(parts)
    if command == "LLEN" and len(parts) >= 2:
        return cmd_llen(parts)
    if command == "LPOP" and len(parts) >= 2:
        return cmd_lpop(parts) or ""
    if command == "BLPOP" and len(parts) >= 3:
        return cmd_blpop(ctx.client, parts)
    if command == "TYPE" and len(parts) >= 2:
        return _cmd_type(parts)
    if command == "WAIT" and len(parts) >= 3:
        return _cmd_wait(parts)
    if command == "PUBLISH" and len(parts) >= 3:
        return cmd_publish(parts)
    if command == "SUBSCRIBE" and len(parts) >= 2:
        cmd_subscribe(ctx.client, parts)
        ctx.is_subscribed_mode = True
        return ""
    if command == "UNSUBSCRIBE" and len(parts) >= 2:
        ctx.is_subscribed_mode = cmd_unsubscribe(ctx.client, parts)
        return ""
    if command == "XADD" and len(parts) >= 4:
        response = cmd_xadd(parts)
        if not response.startswith("-"):
            _notify_key_modified(parts[1], ctx.client)
        return response
    if command == "XREAD" and len(parts) >= 4:
        return cmd_xread(ctx.client, parts)
    if command == "XRANGE" and len(parts) >= 4:
        return cmd_xrange(parts)
    return "-ERR unknown command\r\n"


def handle_client(client: socket.socket) -> None:
    """
    Manage the full lifecycle of a single client connection.

    :param client: Connected client socket.
    """
    ctx = _ClientCtx(client=client)

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

            if ctx.is_subscribed_mode and command not in _SUBSCRIBED_ALLOWED:
                client.send(
                    f"-ERR Can't execute '{command.lower()}': only (P|S)SUBSCRIBE / "
                    f"(P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed "
                    f"in this context\r\n".encode("utf-8")
                )
                continue

            if not ctx.is_authenticated and command not in {"AUTH", "HELLO", "QUIT", "RESET"}:
                client.send(b"-NOAUTH Authentication required.\r\n")
                continue

            response = _dispatch_command(ctx, parts, input_str)
            if response and not ctx.is_replication_connection:
                client.send(response.encode("utf-8"))

        except OSError:
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
    _clear_watch_state(client, ctx.watched_keys)

    client.close()


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
