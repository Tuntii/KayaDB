import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  OP_PUT,
  OP_TXN_BEGIN,
  OP_TXN_COMMIT,
  OP_TXN_OP,
  OP_TXN_ROLLBACK,
  PROTO_VERSION,
  STATUS_TXN_CONFLICT,
  TXN_OP_DELETE,
  TXN_OP_GET,
  TXN_OP_PUT,
  decodeHelloResponse,
  decodeTxnBeginResponse,
  decodeTxnCommitResponse,
  decodeTxnIdPayload,
  decodeTxnOpPayload,
  decodeValuePayload,
  encodeClientFrame,
  encodeHelloRequest,
  encodeKeyPayload,
  encodePutPayload,
  encodeTxnBeginResponse,
  encodeTxnCommitResponse,
  encodeTxnIdPayload,
  encodeTxnOpPayload,
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

  it("txn opcodes match the wire spec", () => {
    assert.equal(OP_TXN_BEGIN, 9);
    assert.equal(OP_TXN_OP, 10);
    assert.equal(OP_TXN_COMMIT, 11);
    assert.equal(OP_TXN_ROLLBACK, 12);
    assert.equal(TXN_OP_GET, 1);
    assert.equal(TXN_OP_PUT, 2);
    assert.equal(TXN_OP_DELETE, 3);
    assert.equal(STATUS_TXN_CONFLICT, 3);
  });

  it("txn begin response roundtrip", () => {
    const cases: Array<[bigint, bigint]> = [
      [0n, 0n],
      [1n, 0n],
      [7n, 42n],
      [0xffff_ffff_ffff_ffffn, 99n],
    ];
    for (const [txnId, snapshotTs] of cases) {
      const encoded = encodeTxnBeginResponse(txnId, snapshotTs);
      const got = decodeTxnBeginResponse(encoded);
      assert.equal(got.txnId, txnId);
      assert.equal(got.snapshotTs, snapshotTs);
    }
    assert.throws(() => decodeTxnBeginResponse(Buffer.alloc(0)));
  });

  it("txn op payload roundtrip (get/put/delete)", () => {
    const get = encodeTxnOpPayload(7n, TXN_OP_GET, Buffer.from("k"));
    let decoded = decodeTxnOpPayload(get);
    assert.equal(decoded.txnId, 7n);
    assert.equal(decoded.op, TXN_OP_GET);
    assert.equal(decoded.key.toString(), "k");
    assert.equal(decoded.value, null);

    const put = encodeTxnOpPayload(2n, TXN_OP_PUT, Buffer.from("k"), Buffer.from("v"));
    decoded = decodeTxnOpPayload(put);
    assert.equal(decoded.txnId, 2n);
    assert.equal(decoded.op, TXN_OP_PUT);
    assert.equal(decoded.key.toString(), "k");
    assert.equal(decoded.value?.toString(), "v");

    const del = encodeTxnOpPayload(3n, TXN_OP_DELETE, Buffer.from("k"));
    decoded = decodeTxnOpPayload(del);
    assert.equal(decoded.txnId, 3n);
    assert.equal(decoded.op, TXN_OP_DELETE);
    assert.equal(decoded.key.toString(), "k");
    assert.equal(decoded.value, null);

    const empty = encodeTxnOpPayload(1n, TXN_OP_PUT, Buffer.alloc(0), Buffer.alloc(0));
    decoded = decodeTxnOpPayload(empty);
    assert.equal(decoded.txnId, 1n);
    assert.equal(decoded.op, TXN_OP_PUT);
    assert.equal(decoded.key.length, 0);
    assert.equal(decoded.value?.length, 0);

    assert.throws(() => decodeTxnOpPayload(Buffer.from([0x01, 0x00, 0x00, 0x00])));
    const bad = encodeTxnOpPayload(1n, 9, Buffer.from("k"));
    assert.throws(() => decodeTxnOpPayload(bad));
  });

  it("txn id payload and commit response roundtrip", () => {
    for (const id of [0n, 1n, 7n, 42n, 0xffff_ffff_ffff_ffffn]) {
      assert.equal(decodeTxnIdPayload(encodeTxnIdPayload(id)), id);
    }
    assert.throws(() => decodeTxnIdPayload(Buffer.alloc(0)));

    for (const ts of [0n, 12n, 99n, 0xffff_ffff_ffff_ffffn]) {
      assert.equal(decodeTxnCommitResponse(encodeTxnCommitResponse(ts)), ts);
    }
    assert.throws(() => decodeTxnCommitResponse(Buffer.alloc(0)));
  });
});
