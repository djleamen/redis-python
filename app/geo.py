"""
Redis-compatible geospatial encoding and distance calculation.
"""

import math
from typing import Tuple

_LAT_RANGE = 170.10225756   # 85.05112878 * 2
_LAT_OFFSET = 85.05112878
_EARTH_RADIUS_M = 6_372_797.560856


def encode_geohash(longitude: float, latitude: float) -> int:
    """
    Encode a WGS-84 coordinate into a 52-bit Redis geohash integer score.

    The result interleaves 26 bits of normalised latitude and 26 bits of
    normalised longitude.

    :param longitude: Longitude in decimal degrees (−180 to 180).
    :param latitude: Latitude in decimal degrees (−85.05 to 85.05).
    :returns: 52-bit geohash integer suitable for use as a sorted-set score.
    """
    norm_lon = (longitude + 180.0) / 360.0
    norm_lat = (latitude + _LAT_OFFSET) / _LAT_RANGE
    limit = (1 << 26) - 1

    lon_bits = max(0, min(limit, int(norm_lon * (1 << 26))))
    lat_bits = max(0, min(limit, int(norm_lat * (1 << 26))))

    result = 0
    for i in range(26):
        result |= ((lat_bits >> i) & 1) << (2 * i)
        result |= ((lon_bits >> i) & 1) << (2 * i + 1)

    return result


def decode_geohash(score: int) -> Tuple[float, float]:
    """
    Decode a Redis geohash integer back into a ``(longitude, latitude)`` pair.

    :param score: 52-bit geohash integer as stored in a sorted set.
    :returns: ``(longitude, latitude)`` tuple in decimal degrees.
    """
    lat_bits = lon_bits = 0
    for i in range(26):
        lat_bits |= ((score >> (2 * i)) & 1) << i
        lon_bits |= ((score >> (2 * i + 1)) & 1) << i

    lon = (lon_bits + 0.5) / float(1 << 26) * 360.0 - 180.0
    lat = (lat_bits + 0.5) / float(1 << 26) * _LAT_RANGE - _LAT_OFFSET
    return lon, lat


def geodist_meters(
    lat1_deg: float, lon1_deg: float, lat2_deg: float, lon2_deg: float
) -> float:
    """
    Return the great-circle distance in metres between two WGS-84 points.

    Uses the Haversine formula with a mean Earth radius of 6 372 797.56 m.

    :param lat1_deg: Latitude of the first point in decimal degrees.
    :param lon1_deg: Longitude of the first point in decimal degrees.
    :param lat2_deg: Latitude of the second point in decimal degrees.
    :param lon2_deg: Longitude of the second point in decimal degrees.
    :returns: Great-circle distance in metres.
    """
    lat1 = math.radians(lat1_deg)
    lat2 = math.radians(lat2_deg)
    dlat = math.radians(lat2_deg - lat1_deg)
    dlon = math.radians(lon2_deg - lon1_deg)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return _EARTH_RADIUS_M * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
