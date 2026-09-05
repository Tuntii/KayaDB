/**
 * Wire codec for the KayaDB client protocol.
 *
 * Byte-compatible with crates/kaya-net and the Python/Go clients.
 * All integers are little-endian. See docs/clients/client-wire-protocol.md.
 */

export const OP_HELLO = 0;
export const OP_PUT = 1;
export const OP_GET = 2;
export const OP_DELETE = 3;
export const OP_SCAN = 4;
export const OP_HEALTH = 5;
export const OP_STATS = 6;
export const OP_TXN_BEGIN = 9;
export const OP_TXN_OP = 10;
export const OP_TXN_COMMIT = 11;
export const OP_TXN_ROLLBACK = 12;
export const OP_CDC_POLL = 13;
export const OP_CDC_CHECKPOINT = 14;
export const OP_LIST_RANGES = 15;

export const TXN_OP_GET = 1;
export const TXN_OP_PUT = 2;
export const TXN_OP_DELETE = 3;

export const STATUS_OK = 0;
export const STATUS_INVALID_ARGUMENT = 1;
export const STATUS_NOT_FOUND = 2;
export const STATUS_TXN_CONFLICT = 3;
export const STATUS_ERROR = 9;
export const STATUS_NOT_LEADER = 10;
export const STATUS_RANGE_MOVED = 11;

export const PROTO_VERSION = 1;
export const CLIENT_AUTH_PREFIX = Buffer.from("CLIENT\x00");

export function encodeClientFrame(opcode: number, payload: Buffer): Buffer {
  const frameLen = 1 + payload.length;
  const out = Buffer.allocUnsafe(4 + frameLen);
  out.writeUInt32LE(frameLen, 0);
  out.writeUInt8(opcode, 4);
  payload.copy(out, 5);
  return out;
}

export function encodePutPayload(key: Buffer, value: Buffer): Buffer {
  const out = Buffer.allocUnsafe(8 + key.length + value.length);
  out.writeUInt32LE(key.length, 0);
  out.writeUInt32LE(value.length, 4);
  key.copy(out, 8);
  value.copy(out, 8 + key.length);
  return out;
}

export function encodeKeyPayload(key: Buffer): Buffer {
  const out = Buffer.allocUnsafe(4 + key.length);
  out.writeUInt32LE(key.length, 0);
  key.copy(out, 4);
  return out;
}

export const encodeScanPayload = encodeKeyPayload;

export function encodeHelloRequest(version: number = PROTO_VERSION): Buffer {
  const out = Buffer.allocUnsafe(2);
  out.writeUInt16LE(version, 0);
  return out;
}

/** Prefix CLIENT\\x00 | u16 len | token when a client token is configured. */
export function wrapClientAuth(inner: Buffer, clientToken?: string | null): Buffer {
  if (clientToken == null || clientToken === "") {
    return inner;
  }
  const tok = Buffer.from(clientToken, "utf8");
  const out = Buffer.allocUnsafe(CLIENT_AUTH_PREFIX.length + 2 + tok.length + inner.length);
  let off = 0;
  CLIENT_AUTH_PREFIX.copy(out, off);
  off += CLIENT_AUTH_PREFIX.length;
  out.writeUInt16LE(tok.length, off);
  off += 2;
  tok.copy(out, off);
  off += tok.length;
  inner.copy(out, off);
  return out;
}

export function decodeValuePayload(data: Buffer): Buffer {
  if (data.length < 4) {
    throw new Error("truncated value payload");
  }
  const vlen = data.readUInt32LE(0);
  if (data.length < 4 + vlen) {
    throw new Error("truncated value bytes");
  }
  return data.subarray(4, 4 + vlen);
}

export function decodeErrorPayload(data: Buffer): string {
  if (data.length >= 4) {
    const mlen = data.readUInt32LE(0);
    const body = data.subarray(4);
    if (body.length >= mlen) {
      return body.subarray(0, mlen).toString("utf8");
    }
  }
  return data.toString("utf8");
}

export function decodeHelloResponse(data: Buffer): number {
  if (data.length < 2) {
    throw new Error("truncated hello response");
  }
  return data.readUInt16LE(0);
}

/** TXN_BEGIN OK: txn_id(u64 LE) | snapshot_ts(u64 LE). */
export function encodeTxnBeginResponse(txnId: bigint, snapshotTs: bigint): Buffer {
  const out = Buffer.allocUnsafe(16);
  out.writeBigUInt64LE(txnId, 0);
  out.writeBigUInt64LE(snapshotTs, 8);
  return out;
}

export function decodeTxnBeginResponse(data: Buffer): { txnId: bigint; snapshotTs: bigint } {
  if (data.length < 16) {
    throw new Error("truncated txn begin response");
  }
  return {
    txnId: data.readBigUInt64LE(0),
    snapshotTs: data.readBigUInt64LE(8),
  };
}

/**
 * TXN_OP request: txn_id(u64) | op(u8) | key_len(u32) | key | [value_len(u32) | value for put].
 */
export function encodeTxnOpPayload(
  txnId: bigint,
  op: number,
  key: Buffer,
  value?: Buffer | null,
): Buffer {
  const valueBytes = value ?? Buffer.alloc(0);
  const size = 8 + 1 + 4 + key.length + (op === TXN_OP_PUT ? 4 + valueBytes.length : 0);
  const out = Buffer.allocUnsafe(size);
  out.writeBigUInt64LE(txnId, 0);
  out.writeUInt8(op, 8);
  out.writeUInt32LE(key.length, 9);
  key.copy(out, 13);
  if (op === TXN_OP_PUT) {
    const off = 13 + key.length;
    out.writeUInt32LE(valueBytes.length, off);
    valueBytes.copy(out, off + 4);
  }
  return out;
}

export function decodeTxnOpPayload(data: Buffer): {
  txnId: bigint;
  op: number;
  key: Buffer;
  value: Buffer | null;
} {
  if (data.length < 8 + 1 + 4) {
    throw new Error("truncated txn op payload");
  }
  const txnId = data.readBigUInt64LE(0);
  const op = data.readUInt8(8);
  const keyLen = data.readUInt32LE(9);
  if (data.length < 13 + keyLen) {
    throw new Error("truncated txn op key");
  }
  const key = Buffer.from(data.subarray(13, 13 + keyLen));
  let cur = 13 + keyLen;
  let value: Buffer | null = null;
  switch (op) {
    case TXN_OP_PUT: {
      if (data.length < cur + 4) {
        throw new Error("truncated txn op value len");
      }
      const valueLen = data.readUInt32LE(cur);
      cur += 4;
      if (data.length < cur + valueLen) {
        throw new Error("truncated txn op value");
      }
      value = Buffer.from(data.subarray(cur, cur + valueLen));
      break;
    }
    case TXN_OP_GET:
    case TXN_OP_DELETE:
      break;
    default:
      throw new Error(`unknown TXN_OP kind: ${op}`);
  }
  return { txnId, op, key, value };
}

/** TXN_COMMIT / TXN_ROLLBACK request: txn_id(u64 LE). */
export function encodeTxnIdPayload(txnId: bigint): Buffer {
  const out = Buffer.allocUnsafe(8);
  out.writeBigUInt64LE(txnId, 0);
  return out;
}

export function decodeTxnIdPayload(data: Buffer): bigint {
  if (data.length < 8) {
    throw new Error("truncated txn id payload");
  }
  return data.readBigUInt64LE(0);
}

/** TXN_COMMIT OK: commit_ts(u64 LE). */
export function encodeTxnCommitResponse(commitTs: bigint): Buffer {
  return encodeTxnIdPayload(commitTs);
}

export function decodeTxnCommitResponse(data: Buffer): bigint {
  if (data.length < 8) {
    throw new Error("truncated txn commit response");
  }
  return data.readBigUInt64LE(0);
}
