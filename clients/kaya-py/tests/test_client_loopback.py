"""Client-level tests against an in-process mock server (framing + redirect)."""

import os
import socket
import struct
import sys
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kaya import KayaClient, codec  # noqa: E402


def _recv_frame(conn):
    header = _recv_exact(conn, 4)
    (frame_len,) = struct.unpack("<I", header)
    rest = _recv_exact(conn, frame_len)
    opcode = rest[0]
    return opcode, rest[1:]


def _recv_exact(conn, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("closed")
        buf.extend(chunk)
    return bytes(buf)


def _send_response(conn, status, payload=b""):
    conn.sendall(struct.pack("<IH", 2 + len(payload), status) + payload)


def _serve(sock, handler):
    conn, _ = sock.accept()
    with conn:
        try:
            while True:
                opcode, payload = _recv_frame(conn)
                if not handler(conn, opcode, payload):
                    break
        except ConnectionError:
            pass


def _start_server(handler):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(4)
    port = sock.getsockname()[1]
    t = threading.Thread(target=_serve, args=(sock, handler), daemon=True)
    t.start()
    return sock, port, t


def test_put_get_roundtrip_over_loopback():
    store = {}

    def handler(conn, opcode, payload):
        if opcode == codec.OP_PUT:
            # Strip optional CLIENT auth prefix.
            payload = _strip_auth(payload)
            klen, vlen = struct.unpack_from("<II", payload, 0)
            key = payload[8 : 8 + klen]
            value = payload[8 + klen : 8 + klen + vlen]
            store[key] = value
            _send_response(conn, codec.STATUS_OK)
        elif opcode == codec.OP_GET:
            payload = _strip_auth(payload)
            (klen,) = struct.unpack_from("<I", payload, 0)
            key = payload[4 : 4 + klen]
            if key in store:
                v = store[key]
                _send_response(conn, codec.STATUS_OK, struct.pack("<I", len(v)) + v)
            else:
                _send_response(conn, codec.STATUS_NOT_FOUND)
        else:
            _send_response(conn, codec.STATUS_ERROR)
        return True

    sock, port, _ = _start_server(handler)
    with sock:
        with KayaClient(f"127.0.0.1:{port}") as client:
            client.put(b"k1", b"v1")
            assert client.get(b"k1") == b"v1"
            assert client.get(b"missing") is None


def test_not_leader_redirect_is_followed():
    # First server redirects to the second (the "leader").
    leader_store = {b"k": b"leader-value"}

    def leader_handler(conn, opcode, payload):
        if opcode == codec.OP_GET:
            _send_response(
                conn, codec.STATUS_OK, struct.pack("<I", len(leader_store[b"k"])) + leader_store[b"k"]
            )
        else:
            _send_response(conn, codec.STATUS_ERROR)
        return True

    lsock, lport, _ = _start_server(leader_handler)

    def follower_handler(conn, opcode, payload):
        hint = f"127.0.0.1:{lport}".encode()
        _send_response(conn, codec.STATUS_NOT_LEADER, hint)
        return True

    fsock, fport, _ = _start_server(follower_handler)
    with lsock, fsock:
        with KayaClient(f"127.0.0.1:{fport}") as client:
            assert client.get(b"k") == b"leader-value"


def _strip_auth(payload):
    if payload.startswith(codec.CLIENT_AUTH_PREFIX):
        payload = payload[len(codec.CLIENT_AUTH_PREFIX) :]
        (tlen,) = struct.unpack_from("<H", payload, 0)
        payload = payload[2 + tlen :]
    return payload
