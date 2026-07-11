"""Wire codec for the KayaDB client protocol.

Byte-exact port of ``crates/kaya-net/src/codec.rs`` + ``transport.rs``. All
integers are little-endian. See ``docs/clients/client-wire-protocol.md``.
"""

from __future__ import annotations

import struct
from typing import List, Optional, Tuple

# Opcodes
OP_HELLO = 0
OP_PUT = 1
OP_GET = 2
OP_DELETE = 3
OP_SCAN = 4
OP_HEALTH = 5
OP_STATS = 6

# Status codes
STATUS_OK = 0
STATUS_INVALID_ARGUMENT = 1
STATUS_NOT_FOUND = 2
STATUS_ERROR = 9
STATUS_NOT_LEADER = 10

PROTO_VERSION = 1
CLIENT_AUTH_PREFIX = b"CLIENT\x00"


def encode_client_frame(opcode: int, payload: bytes) -> bytes:
    """`frame_len(u32) | opcode(u8) | payload`."""
    frame_len = 1 + len(payload)
    return struct.pack("<IB", frame_len, opcode) + payload


def encode_put_payload(key: bytes, value: bytes) -> bytes:
    return struct.pack("<II", len(key), len(value)) + key + value


def encode_key_payload(key: bytes) -> bytes:
    return struct.pack("<I", len(key)) + key


# SCAN uses the same shape as GET/DELETE (prefix in the key field).
encode_scan_payload = encode_key_payload


def encode_hello_request(version: int = PROTO_VERSION) -> bytes:
    return struct.pack("<H", version)


def wrap_client_auth(inner: bytes, client_token: Optional[str]) -> bytes:
    """Prefix ``CLIENT\\x00 | u16 len | token`` when a token is configured."""
    if client_token is None:
        return inner
    tok = client_token.encode("utf-8")
    return CLIENT_AUTH_PREFIX + struct.pack("<H", len(tok)) + tok + inner


def decode_value_payload(data: bytes) -> bytes:
    """GET OK: `value_len(u32) | value`."""
    if len(data) < 4:
        raise ValueError("truncated value payload")
    (vlen,) = struct.unpack_from("<I", data, 0)
    body = data[4:]
    if len(body) < vlen:
        raise ValueError("truncated value bytes")
    return body[:vlen]


def decode_scan_response(data: bytes) -> List[Tuple[bytes, bytes]]:
    """SCAN OK: `count(u32) | [klen|key|vlen|value] * count`."""
    if len(data) < 4:
        raise ValueError("truncated scan response")
    (count,) = struct.unpack_from("<I", data, 0)
    off = 4
    out: List[Tuple[bytes, bytes]] = []
    for _ in range(count):
        if off + 4 > len(data):
            raise ValueError("truncated scan key length")
        (klen,) = struct.unpack_from("<I", data, off)
        off += 4
        key = data[off : off + klen]
        if len(key) < klen:
            raise ValueError("truncated scan key")
        off += klen
        if off + 4 > len(data):
            raise ValueError("truncated scan value length")
        (vlen,) = struct.unpack_from("<I", data, off)
        off += 4
        value = data[off : off + vlen]
        if len(value) < vlen:
            raise ValueError("truncated scan value")
        off += vlen
        out.append((key, value))
    return out


def decode_error_payload(data: bytes) -> str:
    """Status 1/9: `msg_len(u32) | message`. Falls back to raw UTF-8."""
    if len(data) >= 4:
        (mlen,) = struct.unpack_from("<I", data, 0)
        body = data[4:]
        if len(body) >= mlen:
            return body[:mlen].decode("utf-8", "replace")
    return data.decode("utf-8", "replace")


def decode_hello_response(data: bytes) -> int:
    if len(data) < 2:
        raise ValueError("truncated hello response")
    return struct.unpack_from("<H", data, 0)[0]
