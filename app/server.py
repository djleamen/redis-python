"""
Client connection handler and main server loop.
"""

import hashlib
import socket
import threading
import time
from collections import deque
from typing import List
import os

from . import state
from .blocking import unblock_waiting_clients, unblock_waiting_stream_readers
from .commands import execute_command
from .models import BlockedClient, BlockedStreamReader, StoredValue, StreamEntry
from .protocol import parse_resp_array, parse_stream_id
from .rdb import load_rdb_file
from .replication import (
    build_fullresync_payload,
    connect_to_master,
    propagate_to_replicas,
    request_replica_acks,
)
from .utils import get_current_time_ms, parse_args

_WRONGTYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


def _build_stream_resp(
    stream_results: List  # List[Tuple[str, List[StreamEntry]]]
) -> str:
    """
    Encode a list of ``(key, entries)`` stream results into a RESP array.

    :param stream_results: List of ``(stream_key, entries)`` pairs to encode.
    :returns: RESP-encoded nested array string.
    """
    resp = [f"*{len(stream_results)}\r\n"]
    for key, entries in stream_results:
        resp.append("*2\r\n")
        resp.append(f"${len(key)}\r\n{key}\r\n")
        resp.append(f"*{len(entries)}\r\n")
        for entry in entries:
            resp.append("*2\r\n")
            resp.append(f"${len(entry.id)}\r\n{entry.id}\r\n")
            resp.append(f"*{len(entry.fields) * 2}\r\n")
            for fk, fv in entry.fields.items():
                resp.append(f"${len(fk)}\r\n{fk}\r\n")
                resp.append(f"${len(fv)}\r\n{fv}\r\n")
    return "".join(resp)


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

            # ------------------------------------------------------------------
            # Transaction commands
            # ------------------------------------------------------------------
            if command == "MULTI":
                in_transaction = True
                transaction_queue = []
                response = "+OK\r\n"

            elif command == "EXEC":
                if in_transaction:
                    responses = [execute_command(cmd)
                                 for cmd in transaction_queue]
                    response = f"*{len(responses)}\r\n" + "".join(responses)
                    in_transaction = False
                    transaction_queue = []
                else:
                    response = "-ERR EXEC without MULTI\r\n"

            elif command == "DISCARD":
                if in_transaction:
                    in_transaction = False
                    transaction_queue = []
                    response = "+OK\r\n"
                else:
                    response = "-ERR DISCARD without MULTI\r\n"

            elif in_transaction:
                transaction_queue.append(parts)
                response = "+QUEUED\r\n"

            # ------------------------------------------------------------------
            # Pub / sub mode commands
            # ------------------------------------------------------------------
            elif command == "PING" and is_subscribed_mode:
                response = "*2\r\n$4\r\npong\r\n$0\r\n\r\n"

            # ------------------------------------------------------------------
            # Authentication
            # ------------------------------------------------------------------
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

            # ------------------------------------------------------------------
            # Server info / config
            # ------------------------------------------------------------------
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
                if value:
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

            # ------------------------------------------------------------------
            # Replication protocol
            # ------------------------------------------------------------------
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

            # ------------------------------------------------------------------
            # String write (requires propagation)
            # ------------------------------------------------------------------
            elif command == "SET" and len(parts) >= 3:
                response = execute_command(parts)
                if not is_replication_connection:
                    propagate_to_replicas(input_str)

            # ------------------------------------------------------------------
            # Stateless commands — delegate to execute_command
            # ------------------------------------------------------------------
            elif command in {
                "PING", "ECHO", "GET", "INCR",
                "ACL",
                "ZADD", "ZRANK", "ZRANGE", "ZCARD", "ZSCORE", "ZREM",
                "GEOADD", "GEOPOS", "GEODIST", "GEOSEARCH",
            }:
                response = execute_command(parts)

            # ------------------------------------------------------------------
            # List commands
            # ------------------------------------------------------------------
            elif command == "RPUSH" and len(parts) >= 3:
                response, should_unblock = _cmd_rpush(parts)
                if response:
                    client.send(response.encode("utf-8"))
                    response = ""
                if should_unblock:
                    unblock_waiting_clients(parts[1])

            elif command == "LPUSH" and len(parts) >= 3:
                response, should_unblock = _cmd_lpush(parts)
                if response:
                    client.send(response.encode("utf-8"))
                    response = ""
                if should_unblock:
                    unblock_waiting_clients(parts[1])

            elif command == "LRANGE" and len(parts) >= 4:
                response = _cmd_lrange(parts)

            elif command == "LLEN" and len(parts) >= 2:
                response = _cmd_llen(parts)

            elif command == "LPOP" and len(parts) >= 2:
                result = _cmd_lpop(parts)
                if result is None:
                    continue
                response = result

            elif command == "BLPOP" and len(parts) >= 3:
                response = _cmd_blpop(client, parts)

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
                response = _cmd_publish(parts)

            elif command == "SUBSCRIBE" and len(parts) >= 2:
                _cmd_subscribe(client, parts)
                is_subscribed_mode = True
                response = ""

            elif command == "UNSUBSCRIBE" and len(parts) >= 2:
                is_subscribed_mode = _cmd_unsubscribe(client, parts)
                response = ""

            # ------------------------------------------------------------------
            # Stream commands
            # ------------------------------------------------------------------
            elif command == "XADD" and len(parts) >= 4:
                response = _cmd_xadd(parts)

            elif command == "XREAD" and len(parts) >= 4:
                response = _cmd_xread(client, parts)

            elif command == "XRANGE" and len(parts) >= 4:
                response = _cmd_xrange(parts)

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

    client.close()


# ---------------------------------------------------------------------------
# List command implementations
# ---------------------------------------------------------------------------


def _cmd_rpush(parts: List[str]):
    """
    Append elements to the tail of a list.

    :param parts: Parsed RESP command tokens.
    :returns: ``(response, should_unblock)`` tuple.
    """
    key, elements = parts[1], parts[2:]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            lst = list(elements)
            state.data_store[key] = StoredValue(list_value=lst)
            return f":{len(lst)}\r\n", True
        if stored.list_value is None:
            return _WRONGTYPE, False
        stored.list_value.extend(elements)
        return f":{len(stored.list_value)}\r\n", True


def _cmd_lpush(parts: List[str]):
    """
    Prepend elements to the head of a list.

    :param parts: Parsed RESP command tokens.
    :returns: ``(response, should_unblock)`` tuple.
    """
    key, elements = parts[1], parts[2:]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            lst = list(reversed(elements))
            state.data_store[key] = StoredValue(list_value=lst)
            return f":{len(lst)}\r\n", True
        if stored.list_value is None:
            return _WRONGTYPE, False
        for element in elements:
            stored.list_value.insert(0, element)
        return f":{len(stored.list_value)}\r\n", True


def _cmd_lrange(parts: List[str]) -> str:
    """
    Handle the LRANGE command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key = parts[1]
    try:
        start, stop = int(parts[2]), int(parts[3])
    except ValueError:
        return "-ERR value is not an integer or out of range\r\n"

    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            return "*0\r\n"
        if stored.list_value is None:
            return _WRONGTYPE
        lst = stored.list_value
        length = len(lst)
        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = max(0, length + stop)
        if start >= length or start > stop:
            return "*0\r\n"
        stop = min(stop, length - 1)
        items = lst[start: stop + 1]
        resp = f"*{len(items)}\r\n"
        for item in items:
            resp += f"${len(item)}\r\n{item}\r\n"
        return resp


def _cmd_llen(parts: List[str]) -> str:
    """
    Handle the LLEN command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key = parts[1]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            return ":0\r\n"
        if stored.list_value is None:
            return _WRONGTYPE
        return f":{len(stored.list_value)}\r\n"


def _cmd_lpop(parts: List[str]):
    """
    Pop from the head of a list.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string, or ``None`` to skip sending.
    """
    key = parts[1]
    count = 1
    if len(parts) >= 3:
        try:
            count = int(parts[2])
            if count < 1:
                return "-ERR value is not an integer or out of range\r\n"
        except ValueError:
            return "-ERR value is not an integer or out of range\r\n"

    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            return "$-1\r\n"
        if stored.list_value is None:
            return _WRONGTYPE
        if not stored.list_value:
            return "$-1\r\n"

        n = min(count, len(stored.list_value))
        if len(parts) >= 3:
            removed = [stored.list_value.pop(0) for _ in range(n)]
            resp = f"*{len(removed)}\r\n"
            for elem in removed:
                resp += f"${len(elem)}\r\n{elem}\r\n"
            return resp
        else:
            elem = stored.list_value.pop(0)
            return f"${len(elem)}\r\n{elem}\r\n"


def _cmd_blpop(client: socket.socket, parts: List[str]) -> str:
    """
    Handle the BLPOP command, blocking if the list is empty.

    :param client: Connected client socket (used to track blocked state).
    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key = parts[1]
    try:
        timeout = float(parts[2])
    except ValueError:
        return "-ERR timeout is not a float or out of range\r\n"

    should_block = False
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            should_block = True
        elif stored.list_value is None:
            return _WRONGTYPE
        elif stored.list_value:
            elem = stored.list_value.pop(0)
            return f"*2\r\n${len(key)}\r\n{key}\r\n${len(elem)}\r\n{elem}\r\n"
        else:
            should_block = True

    if not should_block:
        return "*-1\r\n"

    event = threading.Event()
    result: list = []
    blocked = BlockedClient(key=key, event=event, result=result)

    with state.blocked_clients_lock:
        if key not in state.blocked_clients:
            state.blocked_clients[key] = deque()
        state.blocked_clients[key].append(blocked)

    if timeout > 0:
        event.wait(timeout=timeout)
    else:
        event.wait()

    if result:
        elem = result[0]
        return f"*2\r\n${len(key)}\r\n{key}\r\n${len(elem)}\r\n{elem}\r\n"

    with state.blocked_clients_lock:
        if key in state.blocked_clients:
            try:
                new_q = deque(
                    bc for bc in state.blocked_clients[key] if bc is not blocked)
                if new_q:
                    state.blocked_clients[key] = new_q
                else:
                    del state.blocked_clients[key]
            except (KeyError, ValueError):
                pass
    return "*-1\r\n"


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
# Pub / sub commands
# ---------------------------------------------------------------------------


def _cmd_publish(parts: List[str]) -> str:
    """
    Handle the PUBLISH command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded integer count of subscribers that received the message.
    """
    channel, message = parts[1], parts[2]
    count = 0
    with state.subscriptions_lock:
        if channel in state.channel_subscribers:
            count = len(state.channel_subscribers[channel])
            msg = (
                f"*3\r\n$7\r\nmessage\r\n"
                f"${len(channel)}\r\n{channel}\r\n"
                f"${len(message)}\r\n{message}\r\n"
            ).encode("utf-8")
            for sub in list(state.channel_subscribers[channel]):
                try:
                    sub.send(msg)
                except (socket.error, OSError):
                    pass
    return f":{count}\r\n"


def _cmd_subscribe(client: socket.socket, parts: List[str]) -> None:
    """
    Register the client for the requested channels and send confirmation messages.

    :param client: Connected client socket.
    :param parts: Parsed RESP command tokens (channels follow the command name).
    """
    with state.subscriptions_lock:
        if client not in state.client_subscriptions:
            state.client_subscriptions[client] = set()
        for channel in parts[1:]:
            state.channel_subscribers.setdefault(channel, set()).add(client)
            state.client_subscriptions[client].add(channel)
            count = len(state.client_subscriptions[client])
            client.send(
                f"*3\r\n$9\r\nsubscribe\r\n"
                f"${len(channel)}\r\n{channel}\r\n:{count}\r\n".encode("utf-8")
            )


def _cmd_unsubscribe(client: socket.socket, parts: List[str]) -> bool:
    """
    Unregister the client from channels.

    :param client: Connected client socket.
    :param parts: Parsed RESP command tokens (channels follow the command name).
    :returns: ``True`` if the client remains subscribed to at least one channel.
    """
    with state.subscriptions_lock:
        for channel in parts[1:]:
            if channel in state.channel_subscribers:
                state.channel_subscribers[channel].discard(client)
                if not state.channel_subscribers[channel]:
                    del state.channel_subscribers[channel]
            if client in state.client_subscriptions:
                state.client_subscriptions[client].discard(channel)
            count = len(state.client_subscriptions.get(client, set()))
            client.send(
                f"*3\r\n$11\r\nunsubscribe\r\n"
                f"${len(channel)}\r\n{channel}\r\n:{count}\r\n".encode("utf-8")
            )
        still_subscribed = bool(state.client_subscriptions.get(client))
        if not still_subscribed and client in state.client_subscriptions:
            del state.client_subscriptions[client]
    return still_subscribed


# ---------------------------------------------------------------------------
# Stream commands
# ---------------------------------------------------------------------------


def _resolve_stream_id(key: str, id_str: str) -> str:
    """
    Resolve ``"$"`` to the last entry ID in a stream, or ``"0-0"`` for empty.

    :param key: Stream key to look up in the data store.
    :param id_str: Raw ID string from the client (e.g. ``"$"`` or ``"0-0"``).
    :returns: Resolved stream entry ID string.
    """
    if id_str != "$":
        return id_str
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored and stored.stream:
            return stored.stream[-1].id
    return "0-0"


def _cmd_xadd(parts: List[str]) -> str:
    """
    Handle the XADD command: append an entry to a stream.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded entry ID string, or an error string.
    """
    key, entry_id = parts[1], parts[2]

    if (len(parts) - 3) % 2 != 0:
        return "-ERR wrong number of arguments for XADD\r\n"

    response = ""
    millis_time = 0
    seq_num = 0

    if entry_id == "*":
        millis_time = get_current_time_ms()
        with state.data_store_lock:
            entries = state.data_store[key].stream if key in state.data_store else None
            if entries:
                last_ms, last_seq = map(int, entries[-1].id.split("-"))
                if millis_time == last_ms:
                    seq_num = last_seq + 1
                elif millis_time <= last_ms:
                    millis_time, seq_num = last_ms, last_seq + 1
        entry_id = f"{millis_time}-{seq_num}"

    else:
        id_parts = entry_id.split("-")
        if len(id_parts) != 2:
            return "-ERR Invalid stream ID specified as stream command argument\r\n"
        try:
            millis_time = int(id_parts[0])
        except ValueError:
            return "-ERR Invalid stream ID specified as stream command argument\r\n"

        if id_parts[1] == "*":
            with state.data_store_lock:
                entries = state.data_store[key].stream if key in state.data_store else None
                if entries:
                    last_ms, last_seq = map(int, entries[-1].id.split("-"))
                    seq_num = last_seq + \
                        1 if millis_time == last_ms else (
                            1 if millis_time == 0 else 0)
                else:
                    seq_num = 1 if millis_time == 0 else 0
            entry_id = f"{millis_time}-{seq_num}"
        else:
            try:
                seq_num = int(id_parts[1])
            except ValueError:
                return "-ERR Invalid stream ID specified as stream command argument\r\n"

    if millis_time == 0 and seq_num == 0:
        return "-ERR The ID specified in XADD must be greater than 0-0\r\n"

    with state.data_store_lock:
        entries = state.data_store[key].stream if key in state.data_store else None
        if entries:
            last_ms, last_seq = map(int, entries[-1].id.split("-"))
            if millis_time < last_ms or (millis_time == last_ms and seq_num <= last_seq):
                return (
                    "-ERR The ID specified in XADD is equal or smaller than"
                    " the target stream top item\r\n"
                )

        fields = {parts[i]: parts[i + 1] for i in range(3, len(parts), 2)}
        entry = StreamEntry(entry_id, fields)

        if key not in state.data_store:
            state.data_store[key] = StoredValue(stream=[entry])
        elif state.data_store[key].stream is not None:
            state.data_store[key].stream.append(entry)  # type: ignore[union-attr]
        else:
            return _WRONGTYPE

    unblock_waiting_stream_readers(key)
    return f"${len(entry_id)}\r\n{entry_id}\r\n"


def _cmd_xread(client: socket.socket, parts: List[str]) -> str:
    """
    Handle the XREAD command, optionally blocking until new entries arrive.

    :param client: Connected client socket (unused directly but kept for signature symmetry).
    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded nested array of stream results, or ``*-1`` if no data.
    """
    block_timeout = -1
    streams_index = 1

    if parts[1].upper() == "BLOCK":
        if len(parts) < 6:
            return "-ERR wrong number of arguments for XREAD\r\n"
        try:
            block_timeout = int(parts[2])
            streams_index = 3
        except ValueError:
            return "-ERR timeout is not an integer or out of range\r\n"

    if parts[streams_index].upper() != "STREAMS":
        return "-ERR wrong number of arguments for XREAD\r\n"

    args_after_streams = len(parts) - streams_index - 1
    if args_after_streams < 2 or args_after_streams % 2 != 0:
        return "-ERR wrong number of arguments for XREAD\r\n"

    stream_count = args_after_streams // 2
    keys_list = [parts[streams_index + 1 + i] for i in range(stream_count)]
    ids = [
        _resolve_stream_id(
            keys_list[i], parts[streams_index + 1 + stream_count + i])
        for i in range(stream_count)
    ]

    def _query_streams():
        results = []
        for k, start_id in zip(keys_list, ids):
            start_ms, start_seq = parse_stream_id(start_id, True)
            with state.data_store_lock:
                stored = state.data_store.get(k)
                if stored is None or stored.stream is None:
                    continue
                matches = [
                    e for e in stored.stream
                    for em, es in [map(int, e.id.split("-"))]
                    if (em, es) > (start_ms, start_seq)
                ]
            if matches:
                results.append((k, matches))
        return results

    stream_results = _query_streams()

    if not stream_results and block_timeout >= 0:
        reader = BlockedStreamReader(keys_list, ids, threading.Event(), [])
        with state.blocked_stream_readers_lock:
            for k in keys_list:
                state.blocked_stream_readers.setdefault(
                    k, deque()).append(reader)

        if block_timeout > 0:
            reader.event.wait(timeout=block_timeout / 1000.0)
        else:
            reader.event.wait()

        with state.blocked_stream_readers_lock:
            for k in keys_list:
                if k in state.blocked_stream_readers:
                    new_q = deque(
                        r for r in state.blocked_stream_readers[k] if r is not reader)
                    if new_q:
                        state.blocked_stream_readers[k] = new_q
                    else:
                        del state.blocked_stream_readers[k]

        if reader.result:
            stream_results = reader.result[0] or []
        else:
            return "*-1\r\n"

    if not stream_results:
        return "*-1\r\n"

    return _build_stream_resp(stream_results)


def _cmd_xrange(parts: List[str]) -> str:
    """
    Handle the XRANGE command: return entries within an ID range.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded nested array of matching stream entries.
    """
    key, start_id, end_id = parts[1], parts[2], parts[3]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            return "*0\r\n"
        if stored.stream is None:
            return _WRONGTYPE
        start_ms, start_seq = parse_stream_id(start_id, True)
        end_ms, end_seq = parse_stream_id(end_id, False)
        matches = []
        for entry in stored.stream:
            em, es = map(int, entry.id.split("-"))
            in_range = False
            if start_ms < em < end_ms:
                in_range = True
            elif em == start_ms == end_ms:
                in_range = start_seq <= es <= end_seq
            elif em == start_ms:
                in_range = es >= start_seq
            elif em == end_ms:
                in_range = es <= end_seq
            if in_range:
                matches.append(entry)

    resp = [f"*{len(matches)}\r\n"]
    for entry in matches:
        resp.append("*2\r\n")
        resp.append(f"${len(entry.id)}\r\n{entry.id}\r\n")
        resp.append(f"*{len(entry.fields) * 2}\r\n")
        for fk, fv in entry.fields.items():
            resp.append(f"${len(fk)}\r\n{fk}\r\n")
            resp.append(f"${len(fv)}\r\n{fv}\r\n")
    return "".join(resp)


# ---------------------------------------------------------------------------
# Server entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """
    Start the Redis server, load persisted data, and accept client connections.
    """

    port, master_host, master_port, dir_path, dbfilename = parse_args()

    state.dir_path = dir_path
    state.dbfilename = dbfilename
    state.master_host = master_host

    load_rdb_file(os.path.join(dir_path, dbfilename))

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
