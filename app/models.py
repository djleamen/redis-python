"""
Data model definitions for the Redis server.
"""

import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class StoredValue:
    """
    A value held in the key-value data store.

    Supports string, list, stream, and sorted-set types. ``expiry_ms`` is an
    absolute Unix timestamp in milliseconds; ``None`` means the key never expires.
    """

    value: Optional[str] = None
    list_value: Optional[List[str]] = None
    stream: Optional[List["StreamEntry"]] = None
    sorted_set: Optional[List["SortedSetEntry"]] = None
    expiry_ms: Optional[int] = None


@dataclass
class StreamEntry:
    """
    A single entry in a Redis stream.

    ``id`` uses the ``"<ms>-<seq>"`` format; ``fields`` holds the key-value payload.
    """

    id: str
    fields: Dict[str, str]


@dataclass
class SortedSetEntry:
    """
    A member of a Redis sorted set with its associated floating-point score.
    """

    member: str
    score: float

    def __lt__(self, other: "SortedSetEntry") -> bool:
        if self.score != other.score:
            return self.score < other.score
        return self.member < other.member

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SortedSetEntry):
            return NotImplemented
        return self.score == other.score and self.member == other.member

    def __hash__(self) -> int:
        return hash(self.member)


@dataclass
class BlockedClient:
    """
    State for a client blocked on ``BLPOP`` waiting for a list element.
    """

    key: str
    event: threading.Event
    result: List[Optional[str]]


@dataclass
class BlockedStreamReader:
    """
    State for a client blocked on ``XREAD BLOCK`` waiting for new stream entries.
    """

    keys: List[str]
    ids: List[str]
    event: threading.Event
    result: List[Optional[List[Tuple[str, List[StreamEntry]]]]]
