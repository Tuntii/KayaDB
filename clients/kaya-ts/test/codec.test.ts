import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  OP_PUT,
  PROTO_VERSION,
  decodeHelloResponse,
  decodeValuePayload,
  encodeClientFrame,
  encodeHelloRequest,
  encodeKeyPayload,
  encodePutPayload,
  wrapClientAuth,
} from "../src/codec.ts";

describe("codec", () => {
  it("PUT frame matches wire-spec example (hello → world)", () => {
    const payload = encodePutPayload(Buffer.from("hello"), Buffer.from("world"));
    const frame = encodeClientFrame(OP_PUT, payload);
    const expected = Buffer.from([
      0x13, 0x00, 0x00, 0x00, // frame_len = 19
      0x01, // opcode PUT
      0x05, 0x00, 0x00, 0x00, // key_len
      0x05, 0x00, 0x00, 0x00, // value_len
      ...Buffer.from("hello"),
      ...Buffer.from("world"),
    ]);
    assert.deepEqual(frame, expected);
  });

  it("value payload roundtrip", () => {
    const body = Buffer.from([0x05, 0x00, 0x00, 0x00, ...Buffer.from("world")]);
    assert.equal(decodeValuePayload(body).toString("utf8"), "world");
  });

  it("client auth prefix when token present", () => {
    const inner = encodeKeyPayload(Buffer.from("k"));
    const wrapped = wrapClientAuth(inner, "tok");
    assert.ok(wrapped.subarray(0, 7).equals(Buffer.from("CLIENT\x00")));
    assert.deepEqual(
      wrapped,
      Buffer.concat([
        Buffer.from("CLIENT\x00"),
        Buffer.from([0x03, 0x00]),
        Buffer.from("tok"),
        inner,
      ]),
    );
    assert.deepEqual(wrapClientAuth(inner, null), inner);
    assert.deepEqual(wrapClientAuth(inner, undefined), inner);
  });

  it("hello request and response", () => {
    assert.deepEqual(encodeHelloRequest(PROTO_VERSION), Buffer.from([0x01, 0x00]));
    assert.equal(decodeHelloResponse(Buffer.from([0x01, 0x00])), 1);
  });
});
