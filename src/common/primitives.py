"""
Shared low-level primitives used across Challenge 3 runtime code.
Keeping these in src/ avoids cross-folder runtime dependencies.
"""

import hashlib
from typing import Tuple


# Reused policy constant from Challenge 2.
DUST_THRESHOLD = 546


def double_sha256(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def read_varint(data: bytes, offset: int) -> Tuple[int, int]:
    """
    Read a Bitcoin varint at offset.
    Returns (value, bytes_consumed).
    """
    first = data[offset]
    if first < 0xFD:
        return first, 1
    if first == 0xFD:
        return int.from_bytes(data[offset + 1:offset + 3], "little"), 3
    if first == 0xFE:
        return int.from_bytes(data[offset + 1:offset + 5], "little"), 5
    return int.from_bytes(data[offset + 1:offset + 9], "little"), 9
