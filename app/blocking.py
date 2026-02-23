"""
Helpers for unblocking clients waiting on list or stream operations.
"""


from . import state
from .protocol import parse_stream_id


def unblock_waiting_clients(key: str) -> None:
    """
    Signal the next blocked ``BLPOP`` client for *key* if an element is available.

    :param key: The list key that may have new elements.
    """
    with state.blocked_clients_lock:
        while key in state.blocked_clients and state.blocked_clients[key]:
            with state.data_store_lock:
                stored = state.data_store.get(key)
                list_value = stored.list_value if stored else None
                if list_value:
                    blocked = state.blocked_clients[key].popleft()
                    blocked.result.append(list_value.pop(0))
                    blocked.event.set()
                    if not state.blocked_clients[key]:
                        del state.blocked_clients[key]
                else:
                    break


def unblock_waiting_stream_readers(key: str) -> None:
    """
    Notify all ``XREAD BLOCK`` clients waiting on *key* that new entries exist.

    :param key: The stream key that may have new entries.
    """
    with state.blocked_stream_readers_lock:
        if key not in state.blocked_stream_readers or not state.blocked_stream_readers[key]:
            return
        readers = list(state.blocked_stream_readers[key])
        state.blocked_stream_readers[key].clear()
        del state.blocked_stream_readers[key]

    for reader in readers:
        results = []
        for i, stream_key in enumerate(reader.keys):
            start_id = reader.ids[i]
            with state.data_store_lock:
                stored = state.data_store.get(stream_key)
                if stored is None or stored.stream is None:
                    continue
                start_ms, start_seq = parse_stream_id(start_id, True)
                matches = []
                for entry in stored.stream:
                    id_parts = entry.id.split("-")
                    em, es = int(id_parts[0]), int(id_parts[1])
                    if (em, es) > (start_ms, start_seq):
                        matches.append(entry)
            if matches:
                results.append((stream_key, matches))

        if results:
            reader.result.append(results)
            reader.event.set()
