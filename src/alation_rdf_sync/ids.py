from __future__ import annotations
import os, time, uuid

def uuid7() -> uuid.UUID:
    """RFC 9562 UUIDv7: 48-bit Unix-ms timestamp + random, monotonic across runs.

    run_id ordering matters for reconcile (`run_id <> :run_id`) and for reasoning
    about which run last touched a row; v7 sorts by creation time, v4 does not.
    """
    ms = time.time_ns() // 1_000_000
    rand = os.urandom(10)
    b = bytearray(16)
    b[0] = (ms >> 40) & 0xFF
    b[1] = (ms >> 32) & 0xFF
    b[2] = (ms >> 24) & 0xFF
    b[3] = (ms >> 16) & 0xFF
    b[4] = (ms >> 8) & 0xFF
    b[5] = ms & 0xFF
    b[6] = 0x70 | (rand[0] & 0x0F)          # version 7
    b[7] = rand[1]
    b[8] = 0x80 | (rand[2] & 0x3F)          # variant 10
    b[9:16] = rand[3:10]
    return uuid.UUID(bytes=bytes(b))
