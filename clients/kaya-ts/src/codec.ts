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

export const STATUS_OK = 0;
export const STATUS_INVALID_ARGUMENT = 1;
export const STATUS_NOT_FOUND = 2;
export const STATUS_ERROR = 9;
export const STATUS_NOT_LEADER = 10;

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
