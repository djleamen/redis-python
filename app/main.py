"""
Redis - A simple Redis server implementation in Python
From CodeCrafters.io build-your-own-redis
"""

import socket
import sys
import threading
import time
import struct
import base64
import os
import hashlib
import math
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import deque


@dataclass
class StoredValue:
    """Store value and expiry time"""
    value: Optional[str] = None
    list_value: Optional[List[str]] = None
    stream: Optional[List['StreamEntry']] = None
    sorted_set: Optional[List['SortedSetEntry']] = None
    expiry_ms: Optional[int] = None


@dataclass
class StreamEntry:
    """Stream entry with ID and key-value pairs"""
    id: str
    fields: Dict[str, str]


@dataclass
class SortedSetEntry:
    """Sorted set entry with member and score"""
    member: str
    score: float

    def __lt__(self, other):
        if self.score != other.score:
            return self.score < other.score
        return self.member < other.member

    def __eq__(self, other):
        return self.score == other.score and self.member == other.member


@dataclass
class BlockedClient:
    """Blocked client waiting for an element from a list"""
    key: str
    event: threading.Event
    result: List[Optional[str]]


@dataclass
class BlockedStreamReader:
    """Blocked stream reader waiting for new entries"""
    keys: List[str]
    ids: List[str]
    event: threading.Event
    result: List[Optional[List[Tuple[str, List[StreamEntry]]]]]


# Global state
data_store: Dict[str, StoredValue] = {}
data_store_lock = threading.Lock()

blocked_clients: Dict[str, deque] = {}
blocked_clients_lock = threading.Lock()

blocked_stream_readers: Dict[str, deque] = {}
blocked_stream_readers_lock = threading.Lock()

channel_subscribers: Dict[str, set] = {}
client_subscriptions: Dict[socket.socket, set] = {}
subscriptions_lock = threading.Lock()

replica_connections: List[socket.socket] = []
replica_connections_lock = threading.Lock()
replica_ack_offsets: Dict[socket.socket, int] = {}

replica_offset = 0
master_offset = 0

REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
REPLICATION_OFFSET = 0

# Default user ACL state
default_user_flags = {"nopass"}
default_user_passwords: List[str] = []

dir_path = "/tmp/redis-files"
dbfilename = "dump.rdb"
master_host = None


def parse_args():
    """Parse command-line arguments"""
    port = 6379
    master_host = None
    master_port = None
    dir_path = "/tmp/redis-files"
    dbfilename = "dump.rdb"

    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == "--port" and i + 1 < len(sys.argv):
            port = int(sys.argv[i + 1])
            i += 2
        elif sys.argv[i] == "--replicaof" and i + 1 < len(sys.argv):
            parts = sys.argv[i + 1].split()
            if len(parts) == 2:
                master_host = parts[0]
                master_port = int(parts[1])
            i += 2
        elif sys.argv[i] == "--dir" and i + 1 < len(sys.argv):
            dir_path = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--dbfilename" and i + 1 < len(sys.argv):
            dbfilename = sys.argv[i + 1]
            i += 2
        else:
            i += 1

    return port, master_host, master_port, dir_path, dbfilename


def get_current_time_ms():
    """Get current time in milliseconds"""
    return int(time.time() * 1000)


def parse_resp_array(input_str: str) -> List[str]:
    """Parse RESP array from input string"""
    parts = []
    lines = input_str.split("\r\n")

    if not lines or not lines[0].startswith('*'):
        return parts

    i = 1
    while i < len(lines):
        if lines[i].startswith('$'):
            i += 1
            if i < len(lines):
                parts.append(lines[i])
                i += 1
        else:
            i += 1

    return parts


def try_parse_resp_command(data: str) -> Tuple[Optional[List[str]], int]:
    """Try to parse a complete RESP command from buffered data"""
    if not data or not data.startswith('*'):
        return None, 0

    lines = data.split("\r\n")

    if len(lines) < 2:
        return None, 0

    try:
        array_length = int(lines[0][1:])
    except ValueError:
        return None, 0

    parts = []
    line_index = 1
    bytes_consumed = len(lines[0]) + 2  # +2 for \r\n

    for i in range(array_length):
        # Check if we have enough lines for bulk string header and value
        if line_index >= len(lines):
            return None, 0

        # Parse bulk string length
        length_line = lines[line_index]
        if not length_line.startswith('$'):
            return None, 0

        try:
            bulk_length = int(length_line[1:])
        except ValueError:
            return None, 0

        bytes_consumed += len(length_line) + 2
        line_index += 1

        # Check if we have the bulk string value
        if line_index >= len(lines):
            return None, 0

        value = lines[line_index]

        # Verify the value length matches
        if len(value) != bulk_length:
            # Check if this is the last line and might be incomplete
            if line_index == len(lines) - 1 or (line_index == len(lines) - 2 and lines[line_index + 1] == ""):
                return None, 0

        parts.append(value)
        bytes_consumed += len(value) + 2
        line_index += 1

    return parts, bytes_consumed


def parse_stream_id(id_str: str, is_start: bool) -> Tuple[int, int]:
    """Parse stream ID into milliseconds and sequence number"""
    if id_str == "-":
        return (0, 0) if is_start else (2**63 - 1, 2**63 - 1)

    if id_str == "+":
        return (2**63 - 1, 2**63 - 1)

    parts = id_str.split('-')
    millis = int(parts[0])

    if len(parts) == 1:
        seq = 0 if is_start else 2**63 - 1
    else:
        seq = int(parts[1])

    return (millis, seq)


def encode_geohash(longitude: float, latitude: float) -> int:
    """Encode latitude/longitude into a 52-bit Redis geohash integer score."""
    norm_lon = (longitude + 180.0) / 360.0
    norm_lat = (latitude + 85.05112878) / 170.10225756

    lon_bits = int(norm_lon * (1 << 26))
    lat_bits = int(norm_lat * (1 << 26))

    lon_bits = max(0, min((1 << 26) - 1, lon_bits))
    lat_bits = max(0, min((1 << 26) - 1, lat_bits))

    result = 0
    for i in range(26):
        result |= ((lat_bits >> i) & 1) << (2 * i)
        result |= ((lon_bits >> i) & 1) << (2 * i + 1)

    return result


def decode_geohash(score: int) -> Tuple[float, float]:
    """Decode a Redis geohash score back to (longitude, latitude)."""
    lat_bits = 0
    lon_bits = 0
    for i in range(26):
        lat_bits |= ((score >> (2 * i)) & 1) << i
        lon_bits |= ((score >> (2 * i + 1)) & 1) << i

    lon = (lon_bits + 0.5) / float(1 << 26) * 360.0 - 180.0
    lat = (lat_bits + 0.5) / float(1 << 26) * 170.10225756 - 85.05112878
    return lon, lat


def geodist_meters(lat1_deg: float, lon1_deg: float, lat2_deg: float, lon2_deg: float) -> float:
    """Calculate distance in meters between two lat/lon points using Haversine formula."""
    earth_radius = 6372797.560856
    lat1 = math.radians(lat1_deg)
    lat2 = math.radians(lat2_deg)
    dlat = math.radians(lat2_deg - lat1_deg)
    dlon = math.radians(lon2_deg - lon1_deg)
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return earth_radius * c


def read_length(data: bytes, offset: int) -> Tuple[int, int]:
    """Read length-encoded value from RDB file"""
    first_byte = data[offset]
    type_bits = (first_byte & 0xC0) >> 6

    if type_bits == 0:
        # 6-bit length
        return (first_byte & 0x3F, 1)
    elif type_bits == 1:
        # 14-bit length
        length = ((first_byte & 0x3F) << 8) | data[offset + 1]
        return (length, 2)
    elif type_bits == 2:
        # 32-bit length (big-endian)
        length = (data[offset + 1] << 24) | (data[offset + 2] << 16) | \
                 (data[offset + 3] << 8) | data[offset + 4]
        return (length, 5)
    else:
        # Special encoding
        return (first_byte & 0x3F, 1)


def read_string(data: bytes, offset: int) -> Tuple[str, int]:
    """Read length-prefixed string from RDB file"""
    length, length_bytes = read_length(data, offset)
    total_bytes_read = length_bytes

    # Check for special encoding
    first_byte = data[offset]
    type_bits = (first_byte & 0xC0) >> 6

    if type_bits == 3:
        # Special encoding - handle integer encoding
        encoding_type = first_byte & 0x3F
        if encoding_type == 0:
            # 8-bit integer
            value = struct.unpack('b', data[offset + 1:offset + 2])[0]
            return (str(value), 2)
        elif encoding_type == 1:
            # 16-bit integer (little-endian)
            value = struct.unpack('<h', data[offset + 1:offset + 3])[0]
            return (str(value), 3)
        elif encoding_type == 2:
            # 32-bit integer (little-endian)
            value = struct.unpack('<i', data[offset + 1:offset + 5])[0]
            return (str(value), 5)

    string = data[offset + length_bytes:offset +
                  length_bytes + length].decode('utf-8')
    total_bytes_read += length
    return (string, total_bytes_read)


def load_rdb_file(file_path: str):
    """Load RDB file and populate data_store"""
    if not os.path.exists(file_path):
        print(f"[RDB] File not found: {file_path}")
        return

    try:
        with open(file_path, 'rb') as f:
            file_bytes = f.read()

        offset = 0

        # Read magic string
        magic = file_bytes[offset:offset + 5].decode('ascii')
        offset += 5

        if magic != "REDIS":
            print("[RDB] Invalid RDB file format")
            return

        # Read version
        version = file_bytes[offset:offset + 4].decode('ascii')
        offset += 4
        print(f"[RDB] Loading RDB version {version}")

        while offset < len(file_bytes):
            opcode = file_bytes[offset]
            offset += 1

            if opcode == 0xFF:
                # End of RDB file
                print("[RDB] Reached end of file")
                break
            elif opcode == 0xFE:
                # Database selector
                db_number, bytes_read = read_length(file_bytes, offset)
                offset += bytes_read
                print(f"[RDB] Selecting database {db_number}")
            elif opcode == 0xFD:
                # Expiry time in seconds
                expiry_seconds = struct.unpack(
                    '<I', file_bytes[offset:offset + 4])[0]
                offset += 4
                expiry_ms = expiry_seconds * 1000

                # Read value type and key-value pair
                value_type = file_bytes[offset]
                offset += 1

                key, key_bytes_read = read_string(file_bytes, offset)
                offset += key_bytes_read

                value, value_bytes_read = read_string(file_bytes, offset)
                offset += value_bytes_read

                with data_store_lock:
                    data_store[key] = StoredValue(
                        value=value, expiry_ms=expiry_ms)
                print(f"[RDB] Loaded key '{key}' with expiry {expiry_ms}ms")
            elif opcode == 0xFC:
                # Expiry time in milliseconds
                expiry_ms = struct.unpack(
                    '<Q', file_bytes[offset:offset + 8])[0]
                offset += 8

                # Read value type and key-value pair
                value_type = file_bytes[offset]
                offset += 1

                key, key_bytes_read = read_string(file_bytes, offset)
                offset += key_bytes_read

                value, value_bytes_read = read_string(file_bytes, offset)
                offset += value_bytes_read

                with data_store_lock:
                    data_store[key] = StoredValue(
                        value=value, expiry_ms=expiry_ms)
                print(f"[RDB] Loaded key '{key}' with expiry {expiry_ms}ms")
            elif opcode == 0xFB:
                # Resizedb - hash table size information
                db_hash_table_size, bytes_read1 = read_length(
                    file_bytes, offset)
                offset += bytes_read1
                expiry_hash_table_size, bytes_read2 = read_length(
                    file_bytes, offset)
                offset += bytes_read2
                print(
                    f"[RDB] Resize DB: hash table size={db_hash_table_size}, expiry hash table size={expiry_hash_table_size}")
            elif opcode == 0xFA:
                # Auxiliary field
                aux_key, key_bytes_read = read_string(file_bytes, offset)
                offset += key_bytes_read
                aux_value, value_bytes_read = read_string(file_bytes, offset)
                offset += value_bytes_read
                print(f"[RDB] Auxiliary field: {aux_key}={aux_value}")
            else:
                # Value type - this is a key-value pair without expiry
                value_type = opcode

                key, key_bytes_read = read_string(file_bytes, offset)
                offset += key_bytes_read

                if value_type == 0:
                    # String encoding
                    value, value_bytes_read = read_string(file_bytes, offset)
                    offset += value_bytes_read

                    with data_store_lock:
                        data_store[key] = StoredValue(value=value)
                    print(f"[RDB] Loaded key '{key}' = '{value}'")
                else:
                    print(f"[RDB] Unsupported value type: {value_type}")
                    break

        print(f"[RDB] Loaded {len(data_store)} keys from RDB file")
    except (IOError, struct.error, ValueError, UnicodeDecodeError) as ex:
        print(f"[RDB] Error loading RDB file: {ex}")


def unblock_waiting_clients(key: str):
    """Unblock waiting clients for a given key"""
    with blocked_clients_lock:
        if key in blocked_clients and len(blocked_clients[key]) > 0:
            while key in blocked_clients and len(blocked_clients[key]) > 0:
                with data_store_lock:
                    list_value = data_store[key].list_value if key in data_store else None
                    if list_value is not None and len(list_value) > 0:
                        blocked_client = blocked_clients[key].popleft()
                        element = list_value.pop(0)

                        blocked_client.result.append(element)
                        blocked_client.event.set()

                        if len(blocked_clients[key]) == 0:
                            del blocked_clients[key]
                    else:
                        break


def unblock_waiting_stream_readers(key: str):
    """Unblock waiting stream readers for a given key"""
    with blocked_stream_readers_lock:
        if key in blocked_stream_readers and len(blocked_stream_readers[key]) > 0:
            readers_to_unblock = list(blocked_stream_readers[key])
            blocked_stream_readers[key].clear()
            del blocked_stream_readers[key]

            for reader in readers_to_unblock:
                results = []

                for i, stream_key in enumerate(reader.keys):
                    start_id = reader.ids[i]

                    with data_store_lock:
                        if stream_key in data_store and data_store[stream_key].stream is not None:
                            start_millis, start_seq = parse_stream_id(
                                start_id, True)
                            matching_entries = []

                            stream_entries = data_store[stream_key].stream
                            if stream_entries is not None:
                                for entry in stream_entries:
                                    id_parts = entry.id.split('-')
                                    entry_millis = int(id_parts[0])
                                    entry_seq = int(id_parts[1])

                                    is_greater = False
                                    if entry_millis > start_millis:
                                        is_greater = True
                                    elif entry_millis == start_millis and entry_seq > start_seq:
                                        is_greater = True

                                    if is_greater:
                                        matching_entries.append(entry)

                            if len(matching_entries) > 0:
                                results.append((stream_key, matching_entries))

                if len(results) > 0:
                    reader.result.append(results)
                    reader.event.set()


def propagate_to_replicas(command: str):
    """Propagate command to all connected replicas"""
    global master_offset
    command_bytes = command.encode('utf-8')

    with replica_connections_lock:
        disconnected_replicas = []

        for replica in replica_connections:
            try:
                replica.send(command_bytes)
            except (socket.error, BrokenPipeError, OSError):
                disconnected_replicas.append(replica)

        # Remove disconnected replicas
        for replica in disconnected_replicas:
            replica_connections.remove(replica)
            if replica in replica_ack_offsets:
                del replica_ack_offsets[replica]

        # Update master offset
        if len(replica_connections) > 0:
            master_offset += len(command_bytes)


def request_replica_acks(replicas: List[socket.socket]):
    """Request ACKs from connected replicas"""
    getack_cmd = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
    getack_bytes = getack_cmd.encode('utf-8')

    with replica_connections_lock:
        disconnected_replicas = []

        for replica in replicas:
            try:
                replica.send(getack_bytes)
            except (socket.error, BrokenPipeError, OSError):
                disconnected_replicas.append(replica)

        for replica in disconnected_replicas:
            if replica in replica_connections:
                replica_connections.remove(replica)
            if replica in replica_ack_offsets:
                del replica_ack_offsets[replica]


def execute_command(parts: List[str]) -> str:
    """Execute a single command and return the response"""
    if len(parts) == 0:
        return "-ERR empty command\r\n"

    command = parts[0].upper()
    response = ""

    # PING and ECHO
    if command == "PING":
        response = "+PONG\r\n"
    elif command == "ECHO" and len(parts) > 1:
        message = parts[1]
        response = f"${len(message)}\r\n{message}\r\n"
    # SET and GET
    elif command == "SET" and len(parts) >= 3:
        key = parts[1]
        value = parts[2]
        expiry_ms = None

        i = 3
        while i < len(parts) - 1:
            option = parts[i].upper()
            if option == "PX":
                expiry_ms = get_current_time_ms() + int(parts[i + 1])
                break
            elif option == "EX":
                expiry_ms = get_current_time_ms() + (int(parts[i + 1]) * 1000)
                break
            i += 1

        with data_store_lock:
            data_store[key] = StoredValue(value=value, expiry_ms=expiry_ms)
        response = "+OK\r\n"
    elif command == "GET" and len(parts) > 1:
        key = parts[1]
        with data_store_lock:
            if key in data_store:
                stored_value = data_store[key]
                if stored_value.expiry_ms and get_current_time_ms() > stored_value.expiry_ms:
                    del data_store[key]
                    response = "$-1\r\n"
                elif stored_value.value is not None:
                    response = f"${len(stored_value.value)}\r\n{stored_value.value}\r\n"
                else:
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            else:
                response = "$-1\r\n"
    # INCR
    elif command == "INCR" and len(parts) >= 2:
        key = parts[1]

        with data_store_lock:
            if key in data_store:
                stored_value = data_store[key]
                if stored_value.expiry_ms and get_current_time_ms() > stored_value.expiry_ms:
                    del data_store[key]
                    data_store[key] = StoredValue(value="1")
                    response = ":1\r\n"
                elif stored_value.value is not None:
                    try:
                        current_value = int(stored_value.value)
                        new_value = current_value + 1
                        data_store[key] = StoredValue(
                            value=str(new_value), expiry_ms=stored_value.expiry_ms)
                        response = f":{new_value}\r\n"
                    except ValueError:
                        response = "-ERR value is not an integer or out of range\r\n"
                else:
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            else:
                data_store[key] = StoredValue(value="1")
                response = ":1\r\n"
    # Sorted sets
    elif command == "ZADD" and len(parts) >= 4:
        key = parts[1]
        try:
            score = float(parts[2])
            member = parts[3]

            with data_store_lock:
                if key not in data_store:
                    sorted_set = [SortedSetEntry(member=member, score=score)]
                    data_store[key] = StoredValue(sorted_set=sorted_set)
                    response = ":1\r\n"
                else:
                    stored_value = data_store[key]
                    if stored_value.sorted_set is not None:
                        # Check if member already exists
                        existing_entry = next(
                            (e for e in stored_value.sorted_set if e.member == member), None)
                        if existing_entry:
                            stored_value.sorted_set.remove(existing_entry)
                            stored_value.sorted_set.append(
                                SortedSetEntry(member=member, score=score))
                            stored_value.sorted_set.sort()
                            response = ":0\r\n"
                        else:
                            stored_value.sorted_set.append(
                                SortedSetEntry(member=member, score=score))
                            stored_value.sorted_set.sort()
                            response = ":1\r\n"
                    else:
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        except ValueError:
            response = "-ERR value is not a valid float\r\n"
    elif command == "GEOADD" and len(parts) >= 5:
        try:
            lon = float(parts[2])
            lat = float(parts[3])
        except ValueError:
            return "-ERR invalid longitude,latitude pair\r\n"

        if lon < -180.0 or lon > 180.0:
            return f"-ERR invalid longitude value {lon:.6f}\r\n"
        if lat < -85.05112878 or lat > 85.05112878:
            return f"-ERR invalid latitude value {lat:.6f}\r\n"

        key = parts[1]
        member = parts[4]
        score = float(encode_geohash(lon, lat))

        with data_store_lock:
            if key not in data_store:
                data_store[key] = StoredValue(sorted_set=[SortedSetEntry(member=member, score=score)])
                response = ":1\r\n"
            else:
                stored_value = data_store[key]
                if stored_value.sorted_set is None:
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                else:
                    existing_entry = next((e for e in stored_value.sorted_set if e.member == member), None)
                    if existing_entry:
                        stored_value.sorted_set.remove(existing_entry)
                        stored_value.sorted_set.append(SortedSetEntry(member=member, score=score))
                        stored_value.sorted_set.sort()
                        response = ":0\r\n"
                    else:
                        stored_value.sorted_set.append(SortedSetEntry(member=member, score=score))
                        stored_value.sorted_set.sort()
                        response = ":1\r\n"
    elif command == "GEOPOS" and len(parts) >= 3:
        key = parts[1]
        member_count = len(parts) - 2
        result_parts = [f"*{member_count}\r\n"]

        with data_store_lock:
            stored_value = data_store.get(key)
            sorted_set = stored_value.sorted_set if stored_value is not None else None

            for member in parts[2:]:
                found = False
                if sorted_set is not None:
                    entry = next((e for e in sorted_set if e.member == member), None)
                    if entry:
                        dec_lon, dec_lat = decode_geohash(int(entry.score))
                        lon_str = format(dec_lon, ".17g")
                        lat_str = format(dec_lat, ".17g")
                        result_parts.append(f"*2\r\n${len(lon_str)}\r\n{lon_str}\r\n${len(lat_str)}\r\n{lat_str}\r\n")
                        found = True
                if not found:
                    result_parts.append("*-1\r\n")

        response = "".join(result_parts)
    elif command == "GEODIST" and len(parts) >= 4:
        key = parts[1]
        member1 = parts[2]
        member2 = parts[3]

        with data_store_lock:
            stored_value = data_store.get(key)
            if stored_value is None or stored_value.sorted_set is None:
                response = "$-1\r\n"
            else:
                e1 = next((e for e in stored_value.sorted_set if e.member == member1), None)
                e2 = next((e for e in stored_value.sorted_set if e.member == member2), None)
                if e1 is None or e2 is None:
                    response = "$-1\r\n"
                else:
                    lon1, lat1 = decode_geohash(int(e1.score))
                    lon2, lat2 = decode_geohash(int(e2.score))
                    dist = geodist_meters(lat1, lon1, lat2, lon2)
                    dist_str = format(dist, ".4f")
                    response = f"${len(dist_str)}\r\n{dist_str}\r\n"
    elif command == "GEOSEARCH" and len(parts) >= 8:
        key = parts[1]
        if parts[2].upper() != "FROMLONLAT" or parts[5].upper() != "BYRADIUS":
            response = "-ERR unsupported GEOSEARCH options\r\n"
        else:
            try:
                center_lon = float(parts[3])
                center_lat = float(parts[4])
                radius = float(parts[6])
            except ValueError:
                return "-ERR invalid arguments\r\n"

            unit_multiplier = {
                "km": 1000.0,
                "mi": 1609.344,
                "ft": 0.3048,
            }.get(parts[7].lower(), 1.0)
            radius_meters = radius * unit_multiplier

            matches: List[str] = []
            with data_store_lock:
                stored_value = data_store.get(key)
                if stored_value is not None and stored_value.sorted_set is not None:
                    for entry in stored_value.sorted_set:
                        member_lon, member_lat = decode_geohash(int(entry.score))
                        dist = geodist_meters(center_lat, center_lon, member_lat, member_lon)
                        if dist <= radius_meters:
                            matches.append(entry.member)

            result_parts = [f"*{len(matches)}\r\n"]
            for member in matches:
                result_parts.append(f"${len(member)}\r\n{member}\r\n")
            response = "".join(result_parts)
    elif command == "AUTH" and len(parts) >= 3:
        username = parts[1]
        password = parts[2]
        if username != "default":
            response = "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
        elif "nopass" in default_user_flags:
            response = "+OK\r\n"
        else:
            password_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
            if password_hash in default_user_passwords:
                response = "+OK\r\n"
            else:
                response = "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
    elif command == "ACL" and len(parts) >= 2 and parts[1].upper() == "WHOAMI":
        response = "$7\r\ndefault\r\n"
    elif command == "ACL" and len(parts) >= 3 and parts[1].upper() == "SETUSER":
        username = parts[2]
        if username != "default":
            response = "-ERR unknown user\r\n"
        else:
            for rule in parts[3:]:
                if rule.startswith(">"):
                    password_hash = hashlib.sha256(rule[1:].encode("utf-8")).hexdigest()
                    if password_hash not in default_user_passwords:
                        default_user_passwords.append(password_hash)
                    default_user_flags.discard("nopass")
            response = "+OK\r\n"
    elif command == "ACL" and len(parts) >= 3 and parts[1].upper() == "GETUSER":
        username = parts[2]
        if username != "default":
            response = "$-1\r\n"
        else:
            flags_list = list(default_user_flags)
            result_parts = ["*4\r\n", "$5\r\nflags\r\n", f"*{len(flags_list)}\r\n"]
            for flag in flags_list:
                result_parts.append(f"${len(flag)}\r\n{flag}\r\n")
            result_parts.append("$9\r\npasswords\r\n")
            result_parts.append(f"*{len(default_user_passwords)}\r\n")
            for password_hash in default_user_passwords:
                result_parts.append(f"${len(password_hash)}\r\n{password_hash}\r\n")
            response = "".join(result_parts)
    elif command == "ZRANK" and len(parts) >= 3:
        key = parts[1]
        member = parts[2]

        with data_store_lock:
            if key not in data_store:
                response = "$-1\r\n"
            else:
                sorted_set = data_store[key].sorted_set
                if sorted_set is None:
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                    return response

                rank = -1
                for i, entry in enumerate(sorted_set):
                    if entry.member == member:
                        rank = i
                        break

                if rank >= 0:
                    response = f":{rank}\r\n"
                else:
                    response = "$-1\r\n"
    elif command == "ZRANGE" and len(parts) >= 4:
        key = parts[1]

        try:
            start = int(parts[2])
            stop = int(parts[3])

            with data_store_lock:
                if key not in data_store:
                    response = "*0\r\n"
                else:
                    sorted_set = data_store[key].sorted_set
                    if sorted_set is None:
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                        return response

                    count = len(sorted_set)

                    if start < 0:
                        start = max(0, count + start)
                    if stop < 0:
                        stop = max(0, count + stop)

                    if start >= count or start > stop:
                        response = "*0\r\n"
                    else:
                        if stop >= count:
                            stop = count - 1

                        result_count = stop - start + 1
                        parts_resp = [f"*{result_count}\r\n"]

                        for i in range(start, stop + 1):
                            member = sorted_set[i].member
                            parts_resp.append(
                                f"${len(member)}\r\n{member}\r\n")

                        response = "".join(parts_resp)
        except ValueError:
            response = "-ERR value is not an integer or out of range\r\n"
    elif command == "ZCARD" and len(parts) >= 2:
        key = parts[1]

        with data_store_lock:
            if key not in data_store:
                response = ":0\r\n"
            else:
                sorted_set = data_store[key].sorted_set
                if sorted_set is None:
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                else:
                    response = f":{len(sorted_set)}\r\n"
    elif command == "ZSCORE" and len(parts) >= 3:
        key = parts[1]
        member = parts[2]

        with data_store_lock:
            if key not in data_store:
                response = "$-1\r\n"
            else:
                sorted_set = data_store[key].sorted_set
                if sorted_set is None:
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                    return response

                entry = next(
                    (e for e in sorted_set if e.member == member), None)
                if entry:
                    score_str = str(entry.score)
                    response = f"${len(score_str)}\r\n{score_str}\r\n"
                else:
                    response = "$-1\r\n"
    elif command == "ZREM" and len(parts) >= 3:
        key = parts[1]
        member = parts[2]

        with data_store_lock:
            if key not in data_store:
                response = ":0\r\n"
            else:
                sorted_set = data_store[key].sorted_set
                if sorted_set is None:
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                    return response

                entry = next(
                    (e for e in sorted_set if e.member == member), None)
                if entry:
                    sorted_set.remove(entry)
                    response = ":1\r\n"
                else:
                    response = ":0\r\n"
    else:
        response = "-ERR unknown command\r\n"

    return response


def process_replicated_command(parts: List[str], stream, command_length: int):
    """Process a command replicated from master"""
    global replica_offset

    if len(parts) == 0:
        return

    command = parts[0].upper()
    offset_before_command = replica_offset

    print(f"[Replica] Processing replicated command: {command}")

    # Handle REPLCONF GETACK
    if command == "REPLCONF" and len(parts) >= 3:
        sub_command = parts[1].upper()
        if sub_command == "GETACK":
            offset_str = str(offset_before_command)
            ack_response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(offset_str)}\r\n{offset_str}\r\n"
            stream.send(ack_response.encode('utf-8'))
            print(
                f"[Replica] Sent ACK response with offset {offset_before_command}")

    # Process write commands
    if command == "SET" and len(parts) >= 3:
        key = parts[1]
        value = parts[2]
        expiry_ms = None

        i = 3
        while i < len(parts) - 1:
            option = parts[i].upper()
            if option == "PX":
                expiry_ms = get_current_time_ms() + int(parts[i + 1])
                break
            elif option == "EX":
                expiry_ms = get_current_time_ms() + (int(parts[i + 1]) * 1000)
                break
            i += 1

        with data_store_lock:
            data_store[key] = StoredValue(value=value, expiry_ms=expiry_ms)
        print(f"[Replica] SET {key} = {value}")

    # Update offset
    replica_offset += command_length
    print(
        f"[Replica] Updated offset from {offset_before_command} to {replica_offset} after {command} ({command_length} bytes)")


def process_buffered_commands(command_buffer: List[str], stream):
    """Process all complete commands from the buffer"""
    buffered_data = "".join(command_buffer)
    processed_length = 0

    print(
        f"[Replica] ProcessBufferedCommands: buffer length = {len(buffered_data)}")
    if len(buffered_data) > 0:
        preview = buffered_data[:200].replace("\r", "\\r").replace("\n", "\\n")
        print(f"[Replica] Buffer content (first 200 chars): {preview}")

    while True:
        remaining_data = buffered_data[processed_length:]

        if len(remaining_data) == 0:
            break

        # Try to parse a complete RESP array command
        command, command_length = try_parse_resp_command(remaining_data)

        if command is None or command_length == 0:
            print(
                f"[Replica] No complete command found, remaining {len(remaining_data)} chars in buffer")
            break

        # Process the command
        process_replicated_command(command, stream, command_length)

        processed_length += command_length

    # Remove processed data from buffer
    if processed_length > 0:
        print(f"[Replica] Removing {processed_length} characters from buffer")
        command_buffer.clear()
        command_buffer.append(buffered_data[processed_length:])


def connect_to_master(host: str, master_port: int, replica_port: int):
    """Connect to master server and perform replication handshake"""
    try:
        master_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_client.connect((host, master_port))

        # Step 1: Send PING
        ping_command = "*1\r\n$4\r\nPING\r\n"
        master_client.send(ping_command.encode('utf-8'))

        response = master_client.recv(4096).decode('utf-8')

        # Step 2: Send REPLCONF listening-port
        port_str = str(replica_port)
        replconf_port = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(port_str)}\r\n{port_str}\r\n"
        master_client.send(replconf_port.encode('utf-8'))

        response = master_client.recv(4096).decode('utf-8')

        # Step 3: Send REPLCONF capa
        replconf_capa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        master_client.send(replconf_capa.encode('utf-8'))

        response = master_client.recv(4096).decode('utf-8')

        # Step 4: Send PSYNC
        psync_command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        master_client.send(psync_command.encode('utf-8'))

        # Step 5: Receive FULLRESYNC and RDB file
        buffer = master_client.recv(4096)
        full_response = buffer.decode('utf-8', errors='ignore')

        fullresync_end = full_response.find("\r\n")
        if fullresync_end == -1:
            print("[Replica] Error: Invalid PSYNC response")
            return

        rdb_start = fullresync_end + 2

        # Continue receiving until we have RDB header
        while rdb_start >= len(full_response) or full_response[rdb_start] != '$':
            additional_data = master_client.recv(4096)
            if len(additional_data) == 0:
                print(
                    "[Replica] Error: Connection closed while waiting for RDB header")
                return
            buffer += additional_data
            full_response = buffer.decode('utf-8', errors='ignore')

        rdb_len_end = full_response.find("\r\n", rdb_start)

        while rdb_len_end == -1:
            additional_data = master_client.recv(4096)
            if len(additional_data) == 0:
                print("[Replica] Error: Connection closed while reading RDB length")
                return
            buffer += additional_data
            full_response = buffer.decode('utf-8', errors='ignore')
            rdb_len_end = full_response.find("\r\n", rdb_start)

        rdb_len_str = full_response[rdb_start + 1:rdb_len_end]
        try:
            rdb_length = int(rdb_len_str)
        except ValueError:
            print("[Replica] Error: Cannot parse RDB length")
            return

        rdb_data_start_in_bytes = len(
            full_response[:rdb_len_end].encode('utf-8')) + 2
        rdb_data_end_in_bytes = rdb_data_start_in_bytes + rdb_length

        while len(buffer) < rdb_data_end_in_bytes:
            additional_data = master_client.recv(4096)
            if len(additional_data) == 0:
                print("[Replica] Error: Connection closed while reading RDB")
                return
            buffer += additional_data

        print(
            f"[Replica] Handshake complete. RDB file received ({rdb_length} bytes). Now processing commands from master...")

        # Step 6: Continue processing commands from master
        command_buffer = []
        if len(buffer) > rdb_data_end_in_bytes:
            leftover_data = buffer[rdb_data_end_in_bytes:].decode(
                'utf-8', errors='ignore')
            command_buffer.append(leftover_data)
            print(
                f"[Replica] Found {len(buffer) - rdb_data_end_in_bytes} bytes of command data after RDB")

        if len(command_buffer) > 0 and len(command_buffer[0]) > 0:
            print(
                f"[Replica] Processing initial buffer with {len(command_buffer[0])} characters")
            process_buffered_commands(command_buffer, master_client)

        while True:
            data = master_client.recv(4096)
            if len(data) == 0:
                print("[Replica] Master connection closed.")
                break

            data_str = data.decode('utf-8', errors='ignore')
            command_buffer.append(data_str)
            print(f"[Replica] Received {len(data)} bytes")

            process_buffered_commands(command_buffer, master_client)
    except (socket.error, ConnectionRefusedError, TimeoutError, OSError) as ex:
        print(f"[Replica] Error in master connection: {ex}")


def handle_client(client: socket.socket):
    """Handle client connection"""
    in_transaction = False
    transaction_queue = []
    is_replication_connection = False
    is_subscribed_mode = False
    is_authenticated = "nopass" in default_user_flags

    while True:
        try:
            buffer = client.recv(1024)

            if len(buffer) == 0:
                break

            # Parse RESP command
            input_str = buffer.decode('utf-8')
            parts = parse_resp_array(input_str)

            if len(parts) == 0:
                continue

            command = parts[0].upper()
            response = ""

            # Handle subscribed mode restrictions
            if is_subscribed_mode:
                allowed_commands = ["SUBSCRIBE", "UNSUBSCRIBE",
                                    "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET"]
                if command not in allowed_commands:
                    response = f"-ERR Can't execute '{command.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
                    client.send(response.encode('utf-8'))
                    continue

            # Check authentication before processing commands
            if not is_authenticated and command not in ["AUTH", "HELLO", "QUIT", "RESET"]:
                response = "-NOAUTH Authentication required.\r\n"
                client.send(response.encode('utf-8'))
                continue

            # MULTI, EXEC, DISCARD
            if command == "MULTI":
                in_transaction = True
                transaction_queue = []
                response = "+OK\r\n"
            elif command == "EXEC":
                if in_transaction:
                    responses = []
                    for queued_command in transaction_queue:
                        cmd_response = execute_command(queued_command)
                        responses.append(cmd_response)

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
            # PING
            elif command == "PING":
                if is_subscribed_mode:
                    response = "*2\r\n$4\r\npong\r\n$0\r\n\r\n"
                else:
                    response = "+PONG\r\n"
            # ECHO
            elif command == "ECHO" and len(parts) > 1:
                message = parts[1]
                response = f"${len(message)}\r\n{message}\r\n"
            # AUTH
            elif command == "AUTH" and len(parts) >= 3:
                username = parts[1]
                password = parts[2]
                if username == "default":
                    if "nopass" in default_user_flags:
                        is_authenticated = True
                        response = "+OK\r\n"
                    else:
                        password_hash = hashlib.sha256(password.encode("utf-8")).hexdigest()
                        if password_hash in default_user_passwords:
                            is_authenticated = True
                            response = "+OK\r\n"
                        else:
                            response = "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
                else:
                    response = "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
            # ACL WHOAMI / SETUSER / GETUSER
            elif command == "ACL" and len(parts) >= 2 and parts[1].upper() == "WHOAMI":
                response = "$7\r\ndefault\r\n"
            elif command == "ACL" and len(parts) >= 3 and parts[1].upper() == "SETUSER":
                username = parts[2]
                if username != "default":
                    response = "-ERR unknown user\r\n"
                else:
                    for rule in parts[3:]:
                        if rule.startswith(">"):
                            password_hash = hashlib.sha256(rule[1:].encode("utf-8")).hexdigest()
                            if password_hash not in default_user_passwords:
                                default_user_passwords.append(password_hash)
                            default_user_flags.discard("nopass")
                    response = "+OK\r\n"
            elif command == "ACL" and len(parts) >= 3 and parts[1].upper() == "GETUSER":
                username = parts[2]
                if username != "default":
                    response = "$-1\r\n"
                else:
                    flags_list = list(default_user_flags)
                    response_parts = ["*4\r\n", "$5\r\nflags\r\n", f"*{len(flags_list)}\r\n"]
                    for flag in flags_list:
                        response_parts.append(f"${len(flag)}\r\n{flag}\r\n")
                    response_parts.append("$9\r\npasswords\r\n")
                    response_parts.append(f"*{len(default_user_passwords)}\r\n")
                    for password_hash in default_user_passwords:
                        response_parts.append(f"${len(password_hash)}\r\n{password_hash}\r\n")
                    response = "".join(response_parts)
            # INFO
            elif command == "INFO":
                is_replica = 'master_host' in globals() and globals().get('master_host') is not None
                role = "slave" if is_replica else "master"

                if len(parts) == 1 or parts[1].upper() == "REPLICATION":
                    if is_replica:
                        info = f"role:{role}"
                    else:
                        info = f"role:{role}\r\nmaster_replid:{REPLICATION_ID}\r\nmaster_repl_offset:{REPLICATION_OFFSET}"
                    response = f"${len(info)}\r\n{info}\r\n"
                else:
                    response = "$0\r\n\r\n"
            # CONFIG GET
            elif command == "CONFIG" and len(parts) >= 3 and parts[1].upper() == "GET":
                parameter = parts[2].lower()
                value = None

                if parameter == "dir":
                    value = dir_path
                elif parameter == "dbfilename":
                    value = dbfilename

                if value:
                    response = f"*2\r\n${len(parameter)}\r\n{parameter}\r\n${len(value)}\r\n{value}\r\n"
                else:
                    response = "*0\r\n"
            # KEYS
            elif command == "KEYS" and len(parts) >= 2:
                pattern = parts[1]
                matching_keys = []

                with data_store_lock:
                    for key, stored_value in data_store.items():
                        if pattern == "*" or key == pattern:
                            if not stored_value.expiry_ms or get_current_time_ms() <= stored_value.expiry_ms:
                                matching_keys.append(key)

                response = f"*{len(matching_keys)}\r\n"
                for key in matching_keys:
                    response += f"${len(key)}\r\n{key}\r\n"
            # REPLCONF
            elif command == "REPLCONF":
                if len(parts) >= 3 and parts[1].upper() == "ACK":
                    try:
                        ack_offset = int(parts[2])
                        with replica_connections_lock:
                            if client in replica_connections:
                                replica_ack_offsets[client] = ack_offset
                    except ValueError:
                        pass
                    response = ""
                else:
                    response = "+OK\r\n"
            # PSYNC
            elif command == "PSYNC" and len(parts) >= 3:
                print("[PSYNC] Starting RDB transfer...")

                fullresync_response = f"+FULLRESYNC {REPLICATION_ID} {REPLICATION_OFFSET}\r\n"

                empty_rdb_base64 = "UkVESVMwMDA5/2NhMOXkSGD0"
                empty_rdb_file = base64.b64decode(empty_rdb_base64)
                rdb_header = f"${len(empty_rdb_file)}\r\n"

                response_data = fullresync_response.encode(
                    'utf-8') + rdb_header.encode('utf-8') + empty_rdb_file

                client.send(response_data)
                print(f"[PSYNC] Sent {len(response_data)} bytes.")

                # Mark this connection as a replica
                with replica_connections_lock:
                    replica_connections.append(client)
                    replica_ack_offsets[client] = 0
                is_replication_connection = True
                continue
            # SET
            elif command == "SET" and len(parts) >= 3:
                key = parts[1]
                value = parts[2]
                expiry_ms = None

                i = 3
                while i < len(parts) - 1:
                    option = parts[i].upper()
                    if option == "PX":
                        expiry_ms = get_current_time_ms() + int(parts[i + 1])
                        break
                    elif option == "EX":
                        expiry_ms = get_current_time_ms(
                        ) + (int(parts[i + 1]) * 1000)
                        break
                    i += 1

                with data_store_lock:
                    data_store[key] = StoredValue(
                        value=value, expiry_ms=expiry_ms)
                response = "+OK\r\n"

                # Propagate to replicas
                if not is_replication_connection:
                    propagate_to_replicas(input_str)
            # GET
            elif command == "GET" and len(parts) > 1:
                key = parts[1]
                with data_store_lock:
                    if key in data_store:
                        stored_value = data_store[key]
                        if stored_value.expiry_ms and get_current_time_ms() > stored_value.expiry_ms:
                            del data_store[key]
                            response = "$-1\r\n"
                        elif stored_value.value is not None:
                            response = f"${len(stored_value.value)}\r\n{stored_value.value}\r\n"
                        else:
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                    else:
                        response = "$-1\r\n"
            # INCR
            elif command == "INCR" and len(parts) >= 2:
                key = parts[1]

                with data_store_lock:
                    if key in data_store:
                        stored_value = data_store[key]
                        if stored_value.expiry_ms and get_current_time_ms() > stored_value.expiry_ms:
                            del data_store[key]
                            response = "-ERR key expired\r\n"
                        elif stored_value.value is not None:
                            try:
                                current_value = int(stored_value.value)
                                new_value = current_value + 1
                                data_store[key] = StoredValue(
                                    value=str(new_value), expiry_ms=stored_value.expiry_ms)
                                response = f":{new_value}\r\n"
                            except ValueError:
                                response = "-ERR value is not an integer or out of range\r\n"
                        else:
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                    else:
                        data_store[key] = StoredValue(value="1")
                        response = ":1\r\n"
            # Sorted sets and geospatials (implemented via execute_command)
            elif command in ["ZADD", "ZRANK", "ZRANGE", "ZCARD", "ZSCORE", "ZREM", "GEOADD", "GEOPOS", "GEODIST", "GEOSEARCH"]:
                response = execute_command(parts)
            # Lists - RPUSH, LPUSH, LRANGE, LLEN, LPOP, BLPOP
            elif command == "RPUSH" and len(parts) >= 3:
                key = parts[1]
                elements = parts[2:]
                should_unblock = False

                with data_store_lock:
                    if key not in data_store:
                        list_value = list(elements)
                        data_store[key] = StoredValue(list_value=list_value)
                        response = f":{len(list_value)}\r\n"
                        should_unblock = True
                    else:
                        stored_value = data_store[key]
                        if stored_value.list_value is not None:
                            stored_value.list_value.extend(elements)
                            response = f":{len(stored_value.list_value)}\r\n"
                            should_unblock = True
                        else:
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

                if response:
                    client.send(response.encode('utf-8'))
                    response = ""

                if should_unblock:
                    unblock_waiting_clients(key)
            elif command == "LPUSH" and len(parts) >= 3:
                key = parts[1]
                elements = parts[2:]
                should_unblock = False

                with data_store_lock:
                    if key not in data_store:
                        list_value = list(reversed(elements))
                        data_store[key] = StoredValue(list_value=list_value)
                        response = f":{len(list_value)}\r\n"
                        should_unblock = True
                    else:
                        stored_value = data_store[key]
                        if stored_value.list_value is not None:
                            for element in elements:
                                stored_value.list_value.insert(0, element)
                            response = f":{len(stored_value.list_value)}\r\n"
                            should_unblock = True
                        else:
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

                if response:
                    client.send(response.encode('utf-8'))
                    response = ""

                if should_unblock:
                    unblock_waiting_clients(key)
            elif command == "LRANGE" and len(parts) >= 4:
                key = parts[1]

                try:
                    start = int(parts[2])
                    stop = int(parts[3])

                    with data_store_lock:
                        if key not in data_store:
                            response = "*0\r\n"
                        elif data_store[key].list_value is None:
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                        else:
                            list_value = data_store[key].list_value
                            if list_value is None:
                                response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                            else:
                                if start < 0:
                                    start = max(0, len(list_value) + start)
                                if stop < 0:
                                    stop = max(0, len(list_value) + stop)

                                if start >= len(list_value) or start > stop:
                                    response = "*0\r\n"
                                else:
                                    actual_stop = min(
                                        stop, len(list_value) - 1)
                                    range_elements = list_value[start:actual_stop + 1]

                                    response = f"*{len(range_elements)}\r\n"
                                    for element in range_elements:
                                        response += f"${len(element)}\r\n{element}\r\n"
                except ValueError:
                    response = "-ERR value is not an integer or out of range\r\n"
            elif command == "LLEN" and len(parts) >= 2:
                key = parts[1]

                with data_store_lock:
                    if key not in data_store:
                        response = ":0\r\n"
                    else:
                        list_value = data_store[key].list_value
                        if list_value is None:
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                        else:
                            response = f":{len(list_value)}\r\n"
            elif command == "LPOP" and len(parts) >= 2:
                key = parts[1]
                count = 1

                if len(parts) >= 3:
                    try:
                        count = int(parts[2])
                        if count < 1:
                            response = "-ERR value is not an integer or out of range\r\n"
                            client.send(response.encode('utf-8'))
                            continue
                    except ValueError:
                        response = "-ERR value is not an integer or out of range\r\n"
                        client.send(response.encode('utf-8'))
                        continue

                with data_store_lock:
                    if key not in data_store:
                        response = "$-1\r\n"
                    else:
                        list_value = data_store[key].list_value
                        if list_value is None:
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                        elif len(list_value) == 0:
                            response = "$-1\r\n"
                        else:
                            elements_to_remove = min(count, len(list_value))

                            if len(parts) >= 3:
                                removed_elements = []
                                for _ in range(elements_to_remove):
                                    removed_elements.append(list_value.pop(0))

                                response = f"*{len(removed_elements)}\r\n"
                                for element in removed_elements:
                                    response += f"${len(element)}\r\n{element}\r\n"
                            else:
                                element = list_value.pop(0)
                                response = f"${len(element)}\r\n{element}\r\n"
            elif command == "BLPOP" and len(parts) >= 3:
                key = parts[1]

                try:
                    timeout = float(parts[2])

                    should_block = False

                    with data_store_lock:
                        if key in data_store:
                            stored_value = data_store[key]
                            if stored_value.list_value is None:
                                response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                            elif len(stored_value.list_value) > 0:
                                element = stored_value.list_value.pop(0)
                                response = f"*2\r\n${len(key)}\r\n{key}\r\n${len(element)}\r\n{element}\r\n"
                            else:
                                should_block = True
                        else:
                            should_block = True

                    if should_block:
                        event = threading.Event()
                        result = []
                        blocked_client = BlockedClient(
                            key=key, event=event, result=result)

                        with blocked_clients_lock:
                            if key not in blocked_clients:
                                blocked_clients[key] = deque()
                            blocked_clients[key].append(blocked_client)

                        # Wait with timeout
                        if timeout > 0:
                            event.wait(timeout=timeout)
                        else:
                            event.wait()

                        if result:
                            popped_element = result[0]
                            response = f"*2\r\n${len(key)}\r\n{key}\r\n${len(popped_element)}\r\n{popped_element}\r\n"
                        else:
                            # Timeout - remove from queue
                            with blocked_clients_lock:
                                if key in blocked_clients:
                                    try:
                                        new_queue = deque(
                                            [bc for bc in blocked_clients[key] if bc != blocked_client])
                                        if new_queue:
                                            blocked_clients[key] = new_queue
                                        else:
                                            del blocked_clients[key]
                                    except (KeyError, ValueError):
                                        pass
                            response = "*-1\r\n"
                except ValueError:
                    response = "-ERR timeout is not a float or out of range\r\n"
            # TYPE
            elif command == "TYPE" and len(parts) >= 2:
                key = parts[1]

                with data_store_lock:
                    if key not in data_store:
                        response = "+none\r\n"
                    else:
                        stored_value = data_store[key]
                        if stored_value.value is not None:
                            if stored_value.expiry_ms and get_current_time_ms() > stored_value.expiry_ms:
                                del data_store[key]
                                response = "+none\r\n"
                            else:
                                response = "+string\r\n"
                        elif stored_value.list_value is not None:
                            response = "+list\r\n"
                        elif stored_value.stream is not None:
                            response = "+stream\r\n"
                        elif stored_value.sorted_set is not None:
                            response = "+zset\r\n"
                        else:
                            response = "+none\r\n"
            # WAIT
            elif command == "WAIT" and len(parts) >= 3:
                try:
                    num_replicas = int(parts[1])
                    timeout = int(parts[2])

                    with replica_connections_lock:
                        replicas = list(replica_connections)
                        current_offset = master_offset

                    if len(replicas) == 0:
                        response = ":0\r\n"
                    elif current_offset == 0:
                        response = f":{len(replicas)}\r\n"
                    else:
                        request_replica_acks(replicas)
                        deadline = time.time() + (timeout / 1000.0)
                        acked_replicas = 0

                        while True:
                            with replica_connections_lock:
                                acked_replicas = sum(1 for replica in replicas
                                                     if replica in replica_ack_offsets and replica_ack_offsets[replica] >= current_offset)

                            if acked_replicas >= num_replicas or time.time() >= deadline:
                                break

                            time.sleep(0.01)

                        response = f":{acked_replicas}\r\n"
                except ValueError:
                    response = "-ERR value is not an integer or out of range\r\n"
            # PUBLISH
            elif command == "PUBLISH" and len(parts) >= 3:
                channel = parts[1]
                message = parts[2]

                subscriber_count = 0
                with subscriptions_lock:
                    if channel in channel_subscribers:
                        subscriber_count = len(channel_subscribers[channel])

                        message_response = f"*3\r\n$7\r\nmessage\r\n${len(channel)}\r\n{channel}\r\n${len(message)}\r\n{message}\r\n"
                        message_bytes = message_response.encode('utf-8')

                        for subscriber in list(channel_subscribers[channel]):
                            try:
                                subscriber.send(message_bytes)
                            except (KeyError, ValueError):
                                pass

                response = f":{subscriber_count}\r\n"
            # SUBSCRIBE
            elif command == "SUBSCRIBE" and len(parts) >= 2:
                with subscriptions_lock:
                    if client not in client_subscriptions:
                        client_subscriptions[client] = set()

                    for i in range(1, len(parts)):
                        channel = parts[i]

                        if channel not in channel_subscribers:
                            channel_subscribers[channel] = set()
                        channel_subscribers[channel].add(client)

                        client_subscriptions[client].add(channel)

                        subscription_count = len(client_subscriptions[client])
                        sub_response = f"*3\r\n$9\r\nsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{subscription_count}\r\n"
                        client.send(sub_response.encode('utf-8'))

                    is_subscribed_mode = True

                response = ""
            # UNSUBSCRIBE
            elif command == "UNSUBSCRIBE" and len(parts) >= 2:
                with subscriptions_lock:
                    for i in range(1, len(parts)):
                        channel = parts[i]

                        if channel in channel_subscribers:
                            channel_subscribers[channel].discard(client)
                            if len(channel_subscribers[channel]) == 0:
                                del channel_subscribers[channel]

                        if client in client_subscriptions:
                            client_subscriptions[client].discard(channel)

                        subscription_count = len(
                            client_subscriptions[client]) if client in client_subscriptions else 0
                        unsub_response = f"*3\r\n$11\r\nunsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{subscription_count}\r\n"
                        client.send(unsub_response.encode('utf-8'))

                    if client not in client_subscriptions or len(client_subscriptions[client]) == 0:
                        is_subscribed_mode = False
                        if client in client_subscriptions:
                            del client_subscriptions[client]

                response = ""
            # XADD - Add entry to a stream
            elif command == "XADD" and len(parts) >= 4:
                key = parts[1]
                entry_id = parts[2]

                field_count = len(parts) - 3
                if field_count % 2 != 0:
                    response = "-ERR wrong number of arguments for XADD\r\n"
                else:
                    millis_time = 0
                    seq_num = 0

                    if entry_id == "*":
                        millis_time = int(get_current_time_ms())

                        with data_store_lock:
                            stream_entries = data_store[key].stream if key in data_store else None
                            if stream_entries is not None and len(stream_entries) > 0:
                                last_entry = stream_entries[-1]
                                last_id_parts = last_entry.id.split('-')
                                last_millis_time = int(last_id_parts[0])
                                last_seq_num = int(last_id_parts[1])

                                if millis_time == last_millis_time:
                                    seq_num = last_seq_num + 1
                                elif millis_time <= last_millis_time:
                                    millis_time = last_millis_time
                                    seq_num = last_seq_num + 1
                                else:
                                    seq_num = 0
                            else:
                                seq_num = 0

                        entry_id = f"{millis_time}-{seq_num}"
                    else:
                        id_parts = entry_id.split('-')
                        if len(id_parts) != 2:
                            response = "-ERR Invalid stream ID specified as stream command argument\r\n"
                        else:
                            try:
                                millis_time = int(id_parts[0])
                            except ValueError:
                                response = "-ERR Invalid stream ID specified as stream command argument\r\n"

                            if not response:
                                if id_parts[1] == "*":
                                    with data_store_lock:
                                        stream_entries = data_store[key].stream if key in data_store else None
                                        if stream_entries is not None and len(stream_entries) > 0:
                                            last_entry = stream_entries[-1]
                                            last_id_parts = last_entry.id.split(
                                                '-')
                                            last_millis_time = int(
                                                last_id_parts[0])
                                            last_seq_num = int(
                                                last_id_parts[1])

                                            if millis_time == last_millis_time:
                                                seq_num = last_seq_num + 1
                                            else:
                                                if millis_time == 0:
                                                    seq_num = 1
                                                else:
                                                    seq_num = 0
                                        else:
                                            if millis_time == 0:
                                                seq_num = 1
                                            else:
                                                seq_num = 0

                                    entry_id = f"{millis_time}-{seq_num}"
                                else:
                                    try:
                                        seq_num = int(id_parts[1])
                                    except ValueError:
                                        response = "-ERR Invalid stream ID specified as stream command argument\r\n"

                    if not response:
                        if millis_time == 0 and seq_num == 0:
                            response = "-ERR The ID specified in XADD must be greater than 0-0\r\n"
                        else:
                            is_valid = True
                            with data_store_lock:
                                stream_entries = data_store[key].stream if key in data_store else None
                                if stream_entries is not None and len(stream_entries) > 0:
                                    last_entry = stream_entries[-1]
                                    last_id_parts = last_entry.id.split('-')
                                    last_millis_time = int(last_id_parts[0])
                                    last_seq_num = int(last_id_parts[1])

                                    if millis_time < last_millis_time:
                                        is_valid = False
                                    elif millis_time == last_millis_time and seq_num <= last_seq_num:
                                        is_valid = False

                                    if not is_valid:
                                        response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"

                            if is_valid:
                                fields = {}
                                for i in range(3, len(parts), 2):
                                    fields[parts[i]] = parts[i + 1]

                                entry = StreamEntry(entry_id, fields)

                                with data_store_lock:
                                    if key not in data_store:
                                        data_store[key] = StoredValue(
                                            stream=[entry])
                                        response = f"${len(entry_id)}\r\n{entry_id}\r\n"
                                    else:
                                        stream_entries = data_store[key].stream
                                        if stream_entries is not None:
                                            stream_entries.append(entry)
                                            response = f"${len(entry_id)}\r\n{entry_id}\r\n"
                                        else:
                                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

                                if response.startswith('$'):
                                    unblock_waiting_stream_readers(key)
            # XREAD - Read data from streams starting from a specified ID (exclusive)
            elif command == "XREAD" and len(parts) >= 4:
                block_timeout = -1
                streams_index = 1

                if parts[1].upper() == "BLOCK":
                    if len(parts) < 6:
                        response = "-ERR wrong number of arguments for XREAD\r\n"
                    else:
                        try:
                            block_timeout = int(parts[2])
                            streams_index = 3
                        except ValueError:
                            response = "-ERR timeout is not an integer or out of range\r\n"

                if not response:
                    if parts[streams_index].upper() != "STREAMS":
                        response = "-ERR wrong number of arguments for XREAD\r\n"
                    elif len(parts) < streams_index + 3:
                        response = "-ERR wrong number of arguments for XREAD\r\n"
                    else:
                        args_after_streams = len(parts) - streams_index - 1
                        if args_after_streams % 2 != 0:
                            response = "-ERR wrong number of arguments for XREAD\r\n"
                        else:
                            stream_count = args_after_streams // 2
                            keys = []
                            ids = []

                            for i in range(stream_count):
                                keys.append(parts[streams_index + 1 + i])
                                id_val = parts[streams_index +
                                               1 + stream_count + i]

                                if id_val == "$":
                                    with data_store_lock:
                                        stream_entries = data_store[keys[i]].stream if keys[i] in data_store else None
                                        if stream_entries is not None and len(stream_entries) > 0:
                                            last_entry = stream_entries[-1]
                                            ids.append(last_entry.id)
                                        else:
                                            ids.append("0-0")
                                else:
                                    ids.append(id_val)

                            # Query each stream and collect results
                            stream_results = []

                            for i in range(stream_count):
                                key = keys[i]
                                start_id = ids[i]

                                with data_store_lock:
                                    stream_entries = data_store[key].stream if key in data_store else None
                                    if stream_entries is not None:
                                        start_millis, start_seq = parse_stream_id(
                                            start_id, True)

                                        matching_entries = []
                                        for entry in stream_entries:
                                            id_parts = entry.id.split('-')
                                            entry_millis = int(id_parts[0])
                                            entry_seq = int(id_parts[1])

                                            is_greater = False
                                            if entry_millis > start_millis:
                                                is_greater = True
                                            elif entry_millis == start_millis and entry_seq > start_seq:
                                                is_greater = True

                                            if is_greater:
                                                matching_entries.append(entry)

                                        if len(matching_entries) > 0:
                                            stream_results.append(
                                                (key, matching_entries))

                            # Build RESP response
                            if len(stream_results) == 0 and block_timeout >= 0:
                                blocked_reader = BlockedStreamReader(
                                    keys, ids, threading.Event(), [])

                                with blocked_stream_readers_lock:
                                    for i in range(stream_count):
                                        key = keys[i]
                                        if key not in blocked_stream_readers:
                                            blocked_stream_readers[key] = deque(
                                            )
                                        blocked_stream_readers[key].append(
                                            blocked_reader)

                                # Wait for timeout or data
                                if block_timeout > 0:
                                    timeout_sec = block_timeout / 1000.0
                                    blocked_reader.event.wait(timeout_sec)
                                else:
                                    blocked_reader.event.wait()

                                # Clean up blocked reader
                                with blocked_stream_readers_lock:
                                    for i in range(stream_count):
                                        key = keys[i]
                                        if key in blocked_stream_readers:
                                            try:
                                                new_queue = deque(
                                                    [r for r in blocked_stream_readers[key] if r != blocked_reader])
                                                if new_queue:
                                                    blocked_stream_readers[key] = new_queue
                                                else:
                                                    del blocked_stream_readers[key]
                                            except (KeyError, ValueError):
                                                pass

                                if len(blocked_reader.result) > 0:
                                    blocked_results = blocked_reader.result[0]
                                    if blocked_results is not None:
                                        stream_results = blocked_results
                                    else:
                                        response = "*-1\r\n"
                                else:
                                    response = "*-1\r\n"

                            if not response:
                                if len(stream_results) == 0:
                                    response = "*-1\r\n"
                                else:
                                    resp_parts = [
                                        f"*{len(stream_results)}\r\n"]

                                    for key, matching_entries in stream_results:
                                        resp_parts.append("*2\r\n")
                                        resp_parts.append(
                                            f"${len(key)}\r\n{key}\r\n")
                                        resp_parts.append(
                                            f"*{len(matching_entries)}\r\n")

                                        for entry in matching_entries:
                                            resp_parts.append("*2\r\n")
                                            resp_parts.append(
                                                f"${len(entry.id)}\r\n{entry.id}\r\n")
                                            resp_parts.append(
                                                f"*{len(entry.fields) * 2}\r\n")
                                            for field_key, field_value in entry.fields.items():
                                                resp_parts.append(
                                                    f"${len(field_key)}\r\n{field_key}\r\n")
                                                resp_parts.append(
                                                    f"${len(field_value)}\r\n{field_value}\r\n")

                                    response = ''.join(resp_parts)
            # XRANGE - Query range of entries from stream
            elif command == "XRANGE" and len(parts) >= 4:
                key = parts[1]
                start_id = parts[2]
                end_id = parts[3]

                with data_store_lock:
                    if key not in data_store:
                        response = "*0\r\n"
                    else:
                        stream_entries = data_store[key].stream
                        if stream_entries is None:
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                        else:
                            start_millis, start_seq = parse_stream_id(
                                start_id, True)
                            end_millis, end_seq = parse_stream_id(end_id, False)

                            matching_entries = []
                            for entry in stream_entries:
                                id_parts = entry.id.split('-')
                                entry_millis = int(id_parts[0])
                                entry_seq = int(id_parts[1])

                                is_in_range = False
                                if entry_millis > start_millis and entry_millis < end_millis:
                                    is_in_range = True
                                elif entry_millis == start_millis and entry_millis == end_millis:
                                    is_in_range = entry_seq >= start_seq and entry_seq <= end_seq
                                elif entry_millis == start_millis:
                                    is_in_range = entry_seq >= start_seq
                                elif entry_millis == end_millis:
                                    is_in_range = entry_seq <= end_seq

                                if is_in_range:
                                    matching_entries.append(entry)

                            resp_parts = [f"*{len(matching_entries)}\r\n"]

                            for entry in matching_entries:
                                resp_parts.append("*2\r\n")
                                resp_parts.append(
                                    f"${len(entry.id)}\r\n{entry.id}\r\n")
                                resp_parts.append(f"*{len(entry.fields) * 2}\r\n")
                                for field_key, field_value in entry.fields.items():
                                    resp_parts.append(
                                        f"${len(field_key)}\r\n{field_key}\r\n")
                                    resp_parts.append(
                                        f"${len(field_value)}\r\n{field_value}\r\n")

                            response = ''.join(resp_parts)
            else:
                response = "-ERR unknown command\r\n"

            if response and not is_replication_connection:
                client.send(response.encode('utf-8'))
        except (socket.error, BrokenPipeError, ConnectionResetError, OSError):
            break

    # Cleanup
    with replica_connections_lock:
        if client in replica_connections:
            replica_connections.remove(client)
        if client in replica_ack_offsets:
            del replica_ack_offsets[client]

    # Clean up subscriptions
    with subscriptions_lock:
        if client in client_subscriptions:
            channels = client_subscriptions[client]
            for channel in channels:
                if channel in channel_subscribers:
                    channel_subscribers[channel].discard(client)
                    if len(channel_subscribers[channel]) == 0:
                        del channel_subscribers[channel]
            del client_subscriptions[client]

    client.close()


def main():
    """Main entry point for the Redis server."""
    global dir_path, dbfilename, master_host

    port, master_host, master_port, dir_path, dbfilename = parse_args()
    is_replica = master_host is not None and master_port is not None

    # Make dir_path and dbfilename global so they can be accessed in CONFIG GET
    globals()['dir_path'] = dir_path
    globals()['dbfilename'] = dbfilename
    globals()['master_host'] = master_host

    # Load RDB file if it exists
    load_rdb_file(os.path.join(dir_path, dbfilename))

    # Create server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", port))
    server_socket.listen(5)

    print(f"Redis server listening on port {port}")

    # Connect to master if this is a replica
    if is_replica:
        threading.Thread(target=connect_to_master, args=(
            master_host, master_port, port), daemon=True).start()

    # Accept client connections
    while True:
        client, addr = server_socket.accept()
        # Disable Nagle's algorithm
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        threading.Thread(target=handle_client, args=(
            client,), daemon=True).start()


if __name__ == "__main__":
    main()
