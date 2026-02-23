"""
RDB persistence: parsing and loading Redis binary dump files.
"""

import os
import struct
from typing import Tuple

from . import state
from .models import StoredValue


def read_length(data: bytes, offset: int) -> Tuple[int, int]:
    """
    Decode a length-encoded integer from an RDB byte buffer.

    :param data: Raw RDB file bytes.
    :param offset: Current read position within *data*.
    :returns: ``(length, bytes_consumed)`` tuple.
    """
    first_byte = data[offset]
    type_bits = (first_byte & 0xC0) >> 6

    if type_bits == 0:
        return first_byte & 0x3F, 1
    elif type_bits == 1:
        return ((first_byte & 0x3F) << 8) | data[offset + 1], 2
    elif type_bits == 2:
        length = (
            (data[offset + 1] << 24)
            | (data[offset + 2] << 16)
            | (data[offset + 3] << 8)
            | data[offset + 4]
        )
        return length, 5
    else:
        return first_byte & 0x3F, 1


def read_string(data: bytes, offset: int) -> Tuple[str, int]:
    """
    Decode a length-prefixed or integer-encoded string from an RDB buffer.

    :param data: Raw RDB file bytes.
    :param offset: Current read position within *data*.
    :returns: ``(decoded_string, bytes_consumed)`` tuple.
    """
    length, length_bytes = read_length(data, offset)
    first_byte = data[offset]
    type_bits = (first_byte & 0xC0) >> 6

    if type_bits == 3:
        encoding_type = first_byte & 0x3F
        if encoding_type == 0:
            return str(struct.unpack("b", data[offset + 1 : offset + 2])[0]), 2
        elif encoding_type == 1:
            return str(struct.unpack("<h", data[offset + 1 : offset + 3])[0]), 3
        elif encoding_type == 2:
            return str(struct.unpack("<i", data[offset + 1 : offset + 5])[0]), 5

    string = data[offset + length_bytes : offset + length_bytes + length].decode("utf-8")
    return string, length_bytes + length


def load_rdb_file(file_path: str) -> None:
    """
    Populate the global data store from an RDB file.

    Silently returns when the file does not exist or cannot be parsed.

    :param file_path: Absolute path to the RDB dump file.
    """
    if not os.path.exists(file_path):
        return

    try:
        with open(file_path, "rb") as f:
            data = f.read()

        if data[:5].decode("ascii") != "REDIS":
            return

        offset = 9  # skip 5-byte magic + 4-byte version string

        while offset < len(data):
            opcode = data[offset]
            offset += 1

            if opcode == 0xFF:
                break

            elif opcode == 0xFE:
                _, br = read_length(data, offset)
                offset += br

            elif opcode == 0xFD:
                expiry_ms = struct.unpack("<I", data[offset : offset + 4])[0] * 1000
                offset += 4
                offset += 1  # value type byte
                key, kb = read_string(data, offset)
                offset += kb
                value, vb = read_string(data, offset)
                offset += vb
                with state.data_store_lock:
                    state.data_store[key] = StoredValue(value=value, expiry_ms=expiry_ms)

            elif opcode == 0xFC:
                expiry_ms = struct.unpack("<Q", data[offset : offset + 8])[0]
                offset += 8
                offset += 1  # value type byte
                key, kb = read_string(data, offset)
                offset += kb
                value, vb = read_string(data, offset)
                offset += vb
                with state.data_store_lock:
                    state.data_store[key] = StoredValue(value=value, expiry_ms=expiry_ms)

            elif opcode == 0xFB:
                _, br1 = read_length(data, offset)
                offset += br1
                _, br2 = read_length(data, offset)
                offset += br2

            elif opcode == 0xFA:
                _, kb = read_string(data, offset)
                offset += kb
                _, vb = read_string(data, offset)
                offset += vb

            else:
                key, kb = read_string(data, offset)
                offset += kb
                if opcode == 0:
                    value, vb = read_string(data, offset)
                    offset += vb
                    with state.data_store_lock:
                        state.data_store[key] = StoredValue(value=value)
                else:
                    break

    except (IOError, struct.error, ValueError, UnicodeDecodeError):
        pass
