"""Synchronous KayaDB client over the TCP client protocol.

Mirrors the Rust reference (``crates/kaya-client``) and the Go client
(``clients/kaya-go``): connection reuse (keep-alive), leader redirect on
``NOT_LEADER``, optional client token, and per-request timeout.
"""

from __future__ import annotations

import socket
import struct
from typing import List, Optional, Tuple

from . import codec


class KayaError(Exception):
    """A KayaDB protocol or transport error."""


class NotFound(KayaError):
    """Raised by ``get`` when the key does not exist (callers may prefer the
    ``get`` return of ``None``; kept for explicit handling)."""


class InvalidArgument(KayaError):
    """The server rejected the request as malformed or out of limits."""


class KayaClient:
    def __init__(
        self,
        addr: str = "127.0.0.1:7379",
        *,
        client_token: Optional[str] = None,
        max_redirects: int = 3,
        timeout: float = 5.0,
    ) -> None:
        self._host, self._port = _parse_addr(addr)
        self.client_token = client_token
        self.max_redirects = max_redirects
        self.timeout = timeout
        self._sock: Optional[socket.socket] = None

    # ── connection management ────────────────────────────────────────────────
    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None

    def __enter__(self) -> "KayaClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _ensure_conn(self) -> socket.socket:
        if self._sock is None:
            sock = socket.create_connection((self._host, self._port), timeout=self.timeout)
            sock.settimeout(self.timeout)
            self._sock = sock
        return self._sock

    def _recv_exact(self, sock: socket.socket, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise KayaError("connection closed by server")
            buf.extend(chunk)
        return bytes(buf)

    def _request_once(self, opcode: int, payload: bytes) -> Tuple[int, bytes]:
        sock = self._ensure_conn()
        sock.sendall(codec.encode_client_frame(opcode, payload))
        (resp_len,) = struct.unpack("<I", self._recv_exact(sock, 4))
        if resp_len < 2:
            raise KayaError("response frame too short")
        (status,) = struct.unpack("<H", self._recv_exact(sock, 2))
        body = self._recv_exact(sock, resp_len - 2) if resp_len > 2 else b""
        return status, body

    def _send(self, opcode: int, payload: bytes) -> Tuple[int, bytes]:
        # Data-path ops (PUT/GET/DELETE/SCAN/STATS) carry the optional token.
        if opcode in (codec.OP_PUT, codec.OP_GET, codec.OP_DELETE, codec.OP_SCAN, codec.OP_STATS):
            payload = codec.wrap_client_auth(payload, self.client_token)

        redirects = 0
        while True:
            try:
                status, body = self._request_once(opcode, payload)
            except (OSError, KayaError):
                # Drop the (possibly half-framed) connection and retry once per
                # redirect budget; a fresh connection re-resolves the leader.
                self.close()
                if redirects >= self.max_redirects:
                    raise
                redirects += 1
                continue

            if status == codec.STATUS_NOT_LEADER:
                self.close()
                if redirects >= self.max_redirects:
                    return status, body
                hint = body.decode("utf-8", "ignore").strip()
                if hint:
                    self._host, self._port = _parse_addr(hint)
                redirects += 1
                continue
            return status, body

    # ── operations ───────────────────────────────────────────────────────────
    def hello(self) -> int:
        status, body = self._send(codec.OP_HELLO, codec.encode_hello_request())
        if status == codec.STATUS_OK:
            return codec.decode_hello_response(body)
        raise _status_error(status, body)

    def put(self, key: bytes, value: bytes) -> None:
        status, body = self._send(codec.OP_PUT, codec.encode_put_payload(key, value))
        if status != codec.STATUS_OK:
            raise _status_error(status, body)

    def get(self, key: bytes) -> Optional[bytes]:
        status, body = self._send(codec.OP_GET, codec.encode_key_payload(key))
        if status == codec.STATUS_OK:
            return codec.decode_value_payload(body)
        if status == codec.STATUS_NOT_FOUND:
            return None
        raise _status_error(status, body)

    def delete(self, key: bytes) -> None:
        status, body = self._send(codec.OP_DELETE, codec.encode_key_payload(key))
        if status != codec.STATUS_OK:
            raise _status_error(status, body)

    def scan(self, prefix: bytes) -> List[Tuple[bytes, bytes]]:
        status, body = self._send(codec.OP_SCAN, codec.encode_scan_payload(prefix))
        if status == codec.STATUS_OK:
            return codec.decode_scan_response(body)
        raise _status_error(status, body)

    def health(self) -> str:
        status, body = self._send(codec.OP_HEALTH, b"")
        if status == codec.STATUS_OK:
            return body.decode("utf-8", "replace")
        raise _status_error(status, body)

    def stats(self) -> str:
        status, body = self._send(codec.OP_STATS, b"")
        if status == codec.STATUS_OK:
            return body.decode("utf-8", "replace")
        raise _status_error(status, body)


def _parse_addr(addr: str) -> Tuple[str, int]:
    host, _, port = addr.rpartition(":")
    if not host or not port:
        raise ValueError(f"invalid address (expected host:port): {addr!r}")
    return host, int(port)


def _status_error(status: int, body: bytes) -> KayaError:
    msg = codec.decode_error_payload(body) if body else f"status {status}"
    if status == codec.STATUS_INVALID_ARGUMENT:
        return InvalidArgument(msg)
    if status == codec.STATUS_NOT_FOUND:
        return NotFound(msg)
    return KayaError(f"status {status}: {msg}")
