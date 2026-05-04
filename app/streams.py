"""
Stream command implementations: XADD, XREAD, XRANGE.
"""

import socket
import threading
from collections import deque
from typing import List

from . import state
from .blocking import unblock_waiting_stream_readers
from .models import BlockedStreamReader, StoredValue, StreamEntry
from .protocol import parse_stream_id
from .utils import get_current_time_ms

_WRONGTYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


def build_stream_resp(stream_results: List) -> str:
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


def resolve_stream_id(key: str, id_str: str) -> str:
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


_ERR_INVALID_ID = "-ERR Invalid stream ID specified as stream command argument\r\n"


def _resolve_full_auto_id(key: str) -> tuple:
    """Resolve a ``*`` entry ID to ``(millis_time, seq_num)``."""
    millis_time = get_current_time_ms()
    seq_num = 0
    with state.data_store_lock:
        entries = state.data_store[key].stream if key in state.data_store else None
        if entries:
            last_ms, last_seq = map(int, entries[-1].id.split("-"))
            if millis_time == last_ms:
                seq_num = last_seq + 1
            elif millis_time <= last_ms:
                millis_time, seq_num = last_ms, last_seq + 1
    return millis_time, seq_num


def _resolve_partial_auto_seq(key: str, millis_time: int) -> int:
    """Resolve a ``ms-*`` entry ID to its sequence number."""
    with state.data_store_lock:
        entries = state.data_store[key].stream if key in state.data_store else None
        if entries:
            last_ms, last_seq = map(int, entries[-1].id.split("-"))
            if millis_time == last_ms:
                return last_seq + 1
            return 1 if millis_time == 0 else 0
        return 1 if millis_time == 0 else 0


def _parse_xadd_id(key: str, entry_id: str):
    """
    Parse and resolve *entry_id* for XADD.

    :returns: ``(entry_id, millis_time, seq_num)`` on success, or an error
        string on failure.
    """
    if entry_id == "*":
        millis_time, seq_num = _resolve_full_auto_id(key)
        return f"{millis_time}-{seq_num}", millis_time, seq_num

    id_parts = entry_id.split("-")
    if len(id_parts) != 2:
        return _ERR_INVALID_ID
    try:
        millis_time = int(id_parts[0])
    except ValueError:
        return _ERR_INVALID_ID

    if id_parts[1] == "*":
        seq_num = _resolve_partial_auto_seq(key, millis_time)
        return f"{millis_time}-{seq_num}", millis_time, seq_num

    try:
        seq_num = int(id_parts[1])
    except ValueError:
        return _ERR_INVALID_ID

    return entry_id, millis_time, seq_num


def _append_stream_entry(
    key: str, entry_id: str, millis_time: int, seq_num: int, fields: dict
) -> str:
    """Validate entry ordering and insert into the stream. Returns RESP response."""
    with state.data_store_lock:
        entries = state.data_store[key].stream if key in state.data_store else None
        if entries:
            last_ms, last_seq = map(int, entries[-1].id.split("-"))
            if millis_time < last_ms or (millis_time == last_ms and seq_num <= last_seq):
                return (
                    "-ERR The ID specified in XADD is equal or smaller than"
                    " the target stream top item\r\n"
                )

        entry = StreamEntry(entry_id, fields)
        if key not in state.data_store:
            state.data_store[key] = StoredValue(stream=[entry])
        elif state.data_store[key].stream is not None:
            state.data_store[key].stream.append(entry)  # type: ignore[union-attr]
        else:
            return _WRONGTYPE

    unblock_waiting_stream_readers(key)
    return f"${len(entry_id)}\r\n{entry_id}\r\n"


def cmd_xadd(parts: List[str]) -> str:
    """
    Handle the XADD command: append an entry to a stream.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded entry ID string, or an error string.
    """
    key, entry_id = parts[1], parts[2]

    if (len(parts) - 3) % 2 != 0:
        return "-ERR wrong number of arguments for XADD\r\n"

    parsed = _parse_xadd_id(key, entry_id)
    if isinstance(parsed, str):
        return parsed
    entry_id, millis_time, seq_num = parsed

    if millis_time == 0 and seq_num == 0:
        return "-ERR The ID specified in XADD must be greater than 0-0\r\n"

    fields = {parts[i]: parts[i + 1] for i in range(3, len(parts), 2)}
    return _append_stream_entry(key, entry_id, millis_time, seq_num, fields)


def cmd_xread(client: socket.socket, parts: List[str]) -> str:
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
        resolve_stream_id(
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

    return build_stream_resp(stream_results)


def cmd_xrange(parts: List[str]) -> str:
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
