"""
List command implementations: RPUSH, LPUSH, LRANGE, LLEN, LPOP, BLPOP.
"""

import socket
import threading
from collections import deque
from typing import List

from . import state
from .blocking import unblock_waiting_clients
from .models import BlockedClient, StoredValue

_WRONGTYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


def cmd_rpush(parts: List[str]) -> str:
    """
    Append elements to the tail of a list.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded integer length of the list after the push.
    """
    key, elements = parts[1], parts[2:]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            lst = list(elements)
            state.data_store[key] = StoredValue(list_value=lst)
            resp = f":{len(lst)}\r\n"
        elif stored.list_value is None:
            return _WRONGTYPE
        else:
            stored.list_value.extend(elements)
            resp = f":{len(stored.list_value)}\r\n"
    unblock_waiting_clients(key)
    return resp


def cmd_lpush(parts: List[str]) -> str:
    """
    Prepend elements to the head of a list.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded integer length of the list after the push.
    """
    key, elements = parts[1], parts[2:]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            lst = list(reversed(elements))
            state.data_store[key] = StoredValue(list_value=lst)
            resp = f":{len(lst)}\r\n"
        elif stored.list_value is None:
            return _WRONGTYPE
        else:
            for element in elements:
                stored.list_value.insert(0, element)
            resp = f":{len(stored.list_value)}\r\n"
    unblock_waiting_clients(key)
    return resp


def cmd_lrange(parts: List[str]) -> str:
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


def cmd_llen(parts: List[str]) -> str:
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


def cmd_lpop(parts: List[str]):
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


def cmd_blpop(client: socket.socket, parts: List[str]) -> str:
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
