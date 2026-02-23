"""
Utilities for parsing the Redis Serialization Protocol (RESP).
"""

from typing import List, Optional, Tuple


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


def try_parse_resp_command(data: str) -> Tuple[Optional[List[str]], int]:
    """
    Attempt to parse a single complete RESP command from buffered data.

    :param data: Accumulated raw data string from the socket buffer.
    :returns: ``(parsed_parts, bytes_consumed)``, or ``(None, 0)`` when no
        complete command is available yet.
    """
    if not data or not data.startswith("*"):
        return None, 0

    lines = data.split("\r\n")
    if len(lines) < 2:
        return None, 0

    try:
        array_length = int(lines[0][1:])
    except ValueError:
        return None, 0

    parts: List[str] = []
    line_index = 1
    bytes_consumed = len(lines[0]) + 2  # +2 for \r\n

    for _ in range(array_length):
        if line_index >= len(lines):
            return None, 0

        length_line = lines[line_index]
        if not length_line.startswith("$"):
            return None, 0

        try:
            bulk_length = int(length_line[1:])
        except ValueError:
            return None, 0

        bytes_consumed += len(length_line) + 2
        line_index += 1

        if line_index >= len(lines):
            return None, 0

        value = lines[line_index]
        if len(value) != bulk_length:
            if line_index == len(lines) - 1 or (
                line_index == len(lines) - 2 and lines[line_index + 1] == ""
            ):
                return None, 0

        parts.append(value)
        bytes_consumed += len(value) + 2
        line_index += 1

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
