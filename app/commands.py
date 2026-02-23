"""
Redis command execution.

``execute_command`` processes a parsed RESP command in isolation — without
per-connection state — and returns a RESP-encoded response string.  It is used
both for commands queued inside a ``MULTI`` transaction and for direct dispatch
from the client handler.
"""

import hashlib
from typing import List

from . import state
from .geo import decode_geohash, encode_geohash, geodist_meters
from .models import SortedSetEntry, StoredValue
from .utils import get_current_time_ms


def execute_command(parts: List[str]) -> str:
    """
    Execute a Redis command and return a RESP-encoded response string.

    This function is stateless with respect to the connection: it does not
    track authentication sessions, pub/sub subscriptions, or replication state.

    :param parts: Parsed RESP command tokens (e.g. ``["SET", "key", "value"]``).
    :returns: RESP-encoded response string.
    """
    if not parts:
        return "-ERR empty command\r\n"

    command = parts[0].upper()

    if command == "PING":
        return "+PONG\r\n"

    if command == "ECHO" and len(parts) > 1:
        msg = parts[1]
        return f"${len(msg)}\r\n{msg}\r\n"

    if command == "SET" and len(parts) >= 3:
        return _cmd_set(parts)

    if command == "GET" and len(parts) > 1:
        return _cmd_get(parts)

    if command == "INCR" and len(parts) >= 2:
        return _cmd_incr(parts)

    if command == "ZADD" and len(parts) >= 4:
        return _cmd_zadd(parts)

    if command == "ZRANK" and len(parts) >= 3:
        return _cmd_zrank(parts)

    if command == "ZRANGE" and len(parts) >= 4:
        return _cmd_zrange(parts)

    if command == "ZCARD" and len(parts) >= 2:
        return _cmd_zcard(parts)

    if command == "ZSCORE" and len(parts) >= 3:
        return _cmd_zscore(parts)

    if command == "ZREM" and len(parts) >= 3:
        return _cmd_zrem(parts)

    if command == "GEOADD" and len(parts) >= 5:
        return _cmd_geoadd(parts)

    if command == "GEOPOS" and len(parts) >= 3:
        return _cmd_geopos(parts)

    if command == "GEODIST" and len(parts) >= 4:
        return _cmd_geodist(parts)

    if command == "GEOSEARCH" and len(parts) >= 8:
        return _cmd_geosearch(parts)

    if command == "AUTH" and len(parts) >= 3:
        return _cmd_auth(parts)

    if command == "ACL" and len(parts) >= 2:
        return _cmd_acl(parts)

    return "-ERR unknown command\r\n"


# ---------------------------------------------------------------------------
# String commands
# ---------------------------------------------------------------------------


def _cmd_set(parts: List[str]) -> str:
    """
    Handle the SET command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key, value = parts[1], parts[2]
    expiry_ms = None
    i = 3
    while i < len(parts) - 1:
        opt = parts[i].upper()
        if opt == "PX":
            expiry_ms = get_current_time_ms() + int(parts[i + 1])
            break
        elif opt == "EX":
            expiry_ms = get_current_time_ms() + int(parts[i + 1]) * 1000
            break
        i += 1
    with state.data_store_lock:
        state.data_store[key] = StoredValue(value=value, expiry_ms=expiry_ms)
    return "+OK\r\n"


def _cmd_get(parts: List[str]) -> str:
    """
    Handle the GET command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key = parts[1]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            return "$-1\r\n"
        if stored.expiry_ms and get_current_time_ms() > stored.expiry_ms:
            del state.data_store[key]
            return "$-1\r\n"
        if stored.value is not None:
            return f"${len(stored.value)}\r\n{stored.value}\r\n"
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


def _cmd_incr(parts: List[str]) -> str:
    """
    Handle the INCR command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key = parts[1]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            state.data_store[key] = StoredValue(value="1")
            return ":1\r\n"
        if stored.expiry_ms and get_current_time_ms() > stored.expiry_ms:
            del state.data_store[key]
            state.data_store[key] = StoredValue(value="1")
            return ":1\r\n"
        if stored.value is None:
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        try:
            new_val = int(stored.value) + 1
            state.data_store[key] = StoredValue(
                value=str(new_val), expiry_ms=stored.expiry_ms
            )
            return f":{new_val}\r\n"
        except ValueError:
            return "-ERR value is not an integer or out of range\r\n"


# ---------------------------------------------------------------------------
# Auth / ACL commands
# ---------------------------------------------------------------------------


def _cmd_auth(parts: List[str]) -> str:
    """
    Handle the AUTH command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    username, password = parts[1], parts[2]
    if username != "default":
        return "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
    if "nopass" in state.default_user_flags:
        return "+OK\r\n"
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    if pw_hash in state.default_user_passwords:
        return "+OK\r\n"
    return "-WRONGPASS invalid username-password pair or user is disabled.\r\n"


def _cmd_acl(parts: List[str]) -> str:
    """
    Handle the ACL command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    sub = parts[1].upper()

    if sub == "WHOAMI":
        return "$7\r\ndefault\r\n"

    if sub == "SETUSER" and len(parts) >= 3:
        if parts[2] != "default":
            return "-ERR unknown user\r\n"
        for rule in parts[3:]:
            if rule.startswith(">"):
                pw_hash = hashlib.sha256(rule[1:].encode()).hexdigest()
                if pw_hash not in state.default_user_passwords:
                    state.default_user_passwords.append(pw_hash)
                state.default_user_flags.discard("nopass")
        return "+OK\r\n"

    if sub == "GETUSER" and len(parts) >= 3:
        if parts[2] != "default":
            return "$-1\r\n"
        flags = list(state.default_user_flags)
        resp = ["*4\r\n", "$5\r\nflags\r\n", f"*{len(flags)}\r\n"]
        for flag in flags:
            resp.append(f"${len(flag)}\r\n{flag}\r\n")
        resp.append("$9\r\npasswords\r\n")
        resp.append(f"*{len(state.default_user_passwords)}\r\n")
        for pw in state.default_user_passwords:
            resp.append(f"${len(pw)}\r\n{pw}\r\n")
        return "".join(resp)

    return "-ERR unknown ACL subcommand\r\n"


# ---------------------------------------------------------------------------
# Sorted set helpers
# ---------------------------------------------------------------------------


def _upsert_sorted_set_entry(key: str, member: str, score: float) -> bool:
    """
    Insert or update a sorted-set member.

    :param key: The sorted-set key in the data store.
    :param member: The member name to insert or update.
    :param score: The floating-point score for the member.
    :returns: ``True`` if the member was newly added, ``False`` if updated.
    """
    stored = state.data_store.get(key)
    if stored is None:
        state.data_store[key] = StoredValue(
            sorted_set=[SortedSetEntry(member=member, score=score)]
        )
        return True
    existing = next((e for e in stored.sorted_set if e.member == member), None)  # type: ignore[union-attr]
    if existing:
        stored.sorted_set.remove(existing)  # type: ignore[union-attr]
    stored.sorted_set.append(SortedSetEntry(member=member, score=score))  # type: ignore[union-attr]
    stored.sorted_set.sort()  # type: ignore[union-attr]
    return existing is None


# ---------------------------------------------------------------------------
# Sorted set commands
# ---------------------------------------------------------------------------


def _cmd_zadd(parts: List[str]) -> str:
    """
    Handle the ZADD command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key = parts[1]
    try:
        score, member = float(parts[2]), parts[3]
    except ValueError:
        return "-ERR value is not a valid float\r\n"
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is not None and stored.sorted_set is None:
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        added = _upsert_sorted_set_entry(key, member, score)
    return f":{1 if added else 0}\r\n"


def _cmd_zrank(parts: List[str]) -> str:
    """
    Handle the ZRANK command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key, member = parts[1], parts[2]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            return "$-1\r\n"
        if stored.sorted_set is None:
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        for i, entry in enumerate(stored.sorted_set):
            if entry.member == member:
                return f":{i}\r\n"
        return "$-1\r\n"


def _cmd_zrange(parts: List[str]) -> str:
    """
    Handle the ZRANGE command.

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
        if stored.sorted_set is None:
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        count = len(stored.sorted_set)
        if start < 0:
            start = max(0, count + start)
        if stop < 0:
            stop = max(0, count + stop)
        if start >= count or start > stop:
            return "*0\r\n"
        stop = min(stop, count - 1)
        result = stored.sorted_set[start : stop + 1]
        resp = [f"*{len(result)}\r\n"]
        for e in result:
            resp.append(f"${len(e.member)}\r\n{e.member}\r\n")
        return "".join(resp)


def _cmd_zcard(parts: List[str]) -> str:
    """
    Handle the ZCARD command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key = parts[1]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            return ":0\r\n"
        if stored.sorted_set is None:
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        return f":{len(stored.sorted_set)}\r\n"

def _cmd_zscore(parts: List[str]) -> str:
    """
    Handle the ZSCORE command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key, member = parts[1], parts[2]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            return "$-1\r\n"
        if stored.sorted_set is None:
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        entry = next((e for e in stored.sorted_set if e.member == member), None)
        if entry is None:
            return "$-1\r\n"
        s = str(entry.score)
        return f"${len(s)}\r\n{s}\r\n"


def _cmd_zrem(parts: List[str]) -> str:
    """
    Handle the ZREM command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key, member = parts[1], parts[2]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None:
            return ":0\r\n"
        if stored.sorted_set is None:
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        entry = next((e for e in stored.sorted_set if e.member == member), None)
        if entry is None:
            return ":0\r\n"
        stored.sorted_set.remove(entry)
        return ":1\r\n"


# ---------------------------------------------------------------------------
# Geospatial commands
# ---------------------------------------------------------------------------


def _cmd_geoadd(parts: List[str]) -> str:
    """
    Handle the GEOADD command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    try:
        lon, lat = float(parts[2]), float(parts[3])
    except ValueError:
        return "-ERR invalid longitude,latitude pair\r\n"
    if not (-180.0 <= lon <= 180.0):
        return f"-ERR invalid longitude value {lon:.6f}\r\n"
    if not (-85.05112878 <= lat <= 85.05112878):
        return f"-ERR invalid latitude value {lat:.6f}\r\n"

    key, member = parts[1], parts[4]
    score = float(encode_geohash(lon, lat))
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is not None and stored.sorted_set is None:
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        added = _upsert_sorted_set_entry(key, member, score)
    return f":{1 if added else 0}\r\n"


def _cmd_geopos(parts: List[str]) -> str:
    """
    Handle the GEOPOS command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key = parts[1]
    resp = [f"*{len(parts) - 2}\r\n"]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        ss = stored.sorted_set if stored else None
        for member in parts[2:]:
            entry = next((e for e in ss if e.member == member), None) if ss else None
            if entry:
                dec_lon, dec_lat = decode_geohash(int(entry.score))
                lon_s = format(dec_lon, ".17g")
                lat_s = format(dec_lat, ".17g")
                resp.append(
                    f"*2\r\n${len(lon_s)}\r\n{lon_s}\r\n${len(lat_s)}\r\n{lat_s}\r\n"
                )
            else:
                resp.append("*-1\r\n")
    return "".join(resp)


def _cmd_geodist(parts: List[str]) -> str:
    """
    Handle the GEODIST command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    key, m1, m2 = parts[1], parts[2], parts[3]
    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None or stored.sorted_set is None:
            return "$-1\r\n"
        e1 = next((e for e in stored.sorted_set if e.member == m1), None)
        e2 = next((e for e in stored.sorted_set if e.member == m2), None)
    if e1 is None or e2 is None:
        return "$-1\r\n"
    lon1, lat1 = decode_geohash(int(e1.score))
    lon2, lat2 = decode_geohash(int(e2.score))
    dist = format(geodist_meters(lat1, lon1, lat2, lon2), ".4f")
    return f"${len(dist)}\r\n{dist}\r\n"


def _cmd_geosearch(parts: List[str]) -> str:
    """
    Handle the GEOSEARCH command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded response string.
    """
    if parts[2].upper() != "FROMLONLAT" or parts[5].upper() != "BYRADIUS":
        return "-ERR unsupported GEOSEARCH options\r\n"
    try:
        center_lon = float(parts[3])
        center_lat = float(parts[4])
        radius = float(parts[6])
    except ValueError:
        return "-ERR invalid arguments\r\n"

    unit_m = {"km": 1000.0, "mi": 1609.344, "ft": 0.3048}.get(parts[7].lower(), 1.0)
    radius_m = radius * unit_m
    key = parts[1]

    with state.data_store_lock:
        stored = state.data_store.get(key)
        if stored is None or stored.sorted_set is None:
            return "*0\r\n"
        matches = []
        for e in stored.sorted_set:
            member_lon, member_lat = decode_geohash(int(e.score))
            if geodist_meters(center_lat, center_lon, member_lat, member_lon) <= radius_m:
                matches.append(e.member)

    resp = [f"*{len(matches)}\r\n"]
    for m in matches:
        resp.append(f"${len(m)}\r\n{m}\r\n")
    return "".join(resp)
