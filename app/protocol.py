"""
Utilities for parsing the Redis Serialization Protocol (RESP).
"""

from typing import List, Optional, Tuple


class RespProtocolError(Exception):
    """Raised when buffered data can never become a valid RESP command."""


def parse_resp_array(input_str: str) -> List[str]:
    """
    Parse a RESP array from a raw input string.

    :param input_str: Raw client data beginning with a RESP ``*`` line.
    :returns: List of argument strings, or an empty list when the input is not
        a valid RESP array.
    """
    parts: List[str] = []
    lines = input_str.split("\r\n")

    if not lines or not lines[0].startswith("*"):
        return parts

    i = 1
    while i < len(lines):
        if lines[i].startswith("$"):
            i += 1
            if i < len(lines):
                parts.append(lines[i])
                i += 1
        else:
            i += 1

    return parts


def _parse_bulk_element(
    lines: List[str], line_index: int
) -> Tuple[str, int, int]:
    """
    Parse one bulk-string element from a split RESP line sequence.

    :param lines: All lines from the raw data buffer split on ``\\r\\n``.
    :param line_index: Index of the ``$<length>`` line for this element.
    :returns: ``(value, next_line_index, bytes_consumed)``
    :raises ValueError: When the element is incomplete (more data may arrive).
    :raises RespProtocolError: When the element is malformed beyond repair.
    """
    if line_index >= len(lines):
        raise ValueError
    length_line = lines[line_index]
    # Lines other than the last are terminated by \r\n, so they can no
    # longer change as more data arrives: reject them outright if invalid.
    line_is_terminated = line_index < len(lines) - 1
    if not length_line.startswith("$"):
        if line_is_terminated:
            raise RespProtocolError
        raise ValueError
    try:
        bulk_length = int(length_line[1:])
    except ValueError:
        if line_is_terminated:
            raise RespProtocolError from None
        raise
    if bulk_length < 0:
        raise RespProtocolError
    line_index += 1
    if line_index >= len(lines):
        raise ValueError
    value = lines[line_index]
    if len(value) != bulk_length:
        if line_index < len(lines) - 1:
            # The value line is terminated, so it can never grow to match
            # its declared length: the stream is corrupt.
            raise RespProtocolError
        raise ValueError
    return value, line_index + 1, len(length_line) + 2 + len(value) + 2


def try_parse_resp_command(data: str) -> Tuple[Optional[List[str]], int]:
    """
    Attempt to parse a single complete RESP command from buffered data.

    :param data: Accumulated raw data string from the socket buffer.
    :returns: ``(parsed_parts, bytes_consumed)``, or ``(None, 0)`` when no
        complete command is available yet.
    :raises RespProtocolError: When the buffered data is malformed and can
        never become a valid command no matter how much more data arrives.
    """
    if not data or not data.startswith("*"):
        return None, 0

    lines = data.split("\r\n")
    if len(lines) < 2:
        return None, 0

    try:
        array_length = int(lines[0][1:])
    except ValueError:
        # The header line is already terminated by \r\n, so it is garbage.
        raise RespProtocolError from None
    if array_length < 0:
        raise RespProtocolError

    parts: List[str] = []
    line_index = 1
    bytes_consumed = len(lines[0]) + 2  # +2 for \r\n

    try:
        for _ in range(array_length):
            value, line_index, extra = _parse_bulk_element(lines, line_index)
            parts.append(value)
            bytes_consumed += extra
    except ValueError:
        return None, 0

    return parts, bytes_consumed


def parse_stream_id(id_str: str, is_start: bool) -> Tuple[int, int]:
    """
    Parse a stream entry ID string into a ``(milliseconds, sequence)`` tuple.

    Special values:

    * ``"-"`` → the minimum boundary ``(0, 0)`` or the maximum when not a start.
    * ``"+"`` → the maximum boundary.

    For an ID with only a millisecond part the sequence defaults to ``0``
    (start boundary) or ``2**63 - 1`` (end boundary).

    :param id_str: Stream entry ID string, e.g. ``"1234567890-0"``, ``"-"``, or ``"+"``.
    :param is_start: When ``True``, treat a partial ID as the start of a range.
    :returns: ``(milliseconds, sequence)`` integer pair.
    """
    _MAX = 2**63 - 1

    if id_str == "-":
        return (0, 0) if is_start else (_MAX, _MAX)
    if id_str == "+":
        return (_MAX, _MAX)

    parts = id_str.split("-")
    millis = int(parts[0])
    if len(parts) == 1:
        seq = 0 if is_start else _MAX
    else:
        seq = int(parts[1])

    return millis, seq
