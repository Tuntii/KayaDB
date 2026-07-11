"""Byte-exact codec tests against the wire spec examples."""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kaya import codec  # noqa: E402


def test_put_frame_matches_wire_spec_example():
    # docs/clients/client-wire-protocol.md §5: PUT "hello" -> "world".
    payload = codec.encode_put_payload(b"hello", b"world")
    frame = codec.encode_client_frame(codec.OP_PUT, payload)
    assert frame == (
        b"\x13\x00\x00\x00"  # frame_len = 19 (1 opcode + 18 payload)
        b"\x01"  # opcode PUT
        b"\x05\x00\x00\x00"  # key_len
        b"\x05\x00\x00\x00"  # value_len
        b"hello"
        b"world"
    )


def test_value_payload_roundtrip():
    assert codec.decode_value_payload(b"\x05\x00\x00\x00world") == b"world"


def test_scan_response_roundtrip():
    body = (
        b"\x02\x00\x00\x00"  # count = 2
        b"\x01\x00\x00\x00a\x02\x00\x00\x00xy"
        b"\x01\x00\x00\x00b\x01\x00\x00\x00z"
    )
    assert codec.decode_scan_response(body) == [(b"a", b"xy"), (b"b", b"z")]


def test_client_auth_prefix_when_token_present():
    inner = codec.encode_key_payload(b"k")
    wrapped = codec.wrap_client_auth(inner, "tok")
    assert wrapped.startswith(b"CLIENT\x00")
    # CLIENT\x00 | u16(3) | "tok" | inner
    assert wrapped == b"CLIENT\x00\x03\x00tok" + inner
    assert codec.wrap_client_auth(inner, None) == inner


def test_error_payload_decode_with_length_prefix():
    body = b"\x03\x00\x00\x00bad"
    assert codec.decode_error_payload(body) == "bad"


def test_hello_request_and_response():
    assert codec.encode_hello_request(1) == b"\x01\x00"
    assert codec.decode_hello_response(b"\x01\x00") == 1
