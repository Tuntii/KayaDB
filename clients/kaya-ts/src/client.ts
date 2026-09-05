/**
 * Async KayaDB TCP client using Node net.Socket.
 *
 * Mirrors the Python and Go clients: connection reuse, NOT_LEADER redirect,
 * RetryPolicy (exp backoff + jitter), optional client token, and TXN opcodes.
 */

import * as net from "node:net";
import {
  OP_DELETE,
  OP_GET,
  OP_HEALTH,
  OP_HELLO,
  OP_PUT,
  OP_TXN_BEGIN,
  OP_TXN_COMMIT,
  OP_TXN_OP,
  OP_TXN_ROLLBACK,
  STATUS_INVALID_ARGUMENT,
  STATUS_NOT_FOUND,
  STATUS_NOT_LEADER,
  STATUS_OK,
  STATUS_TXN_CONFLICT,
  TXN_OP_DELETE,
  TXN_OP_GET,
  TXN_OP_PUT,
  decodeErrorPayload,
  decodeHelloResponse,
  decodeTxnBeginResponse,
  decodeTxnCommitResponse,
  decodeValuePayload,
  encodeClientFrame,
  encodeHelloRequest,
  encodeKeyPayload,
  encodePutPayload,
  encodeTxnIdPayload,
  encodeTxnOpPayload,
  wrapClientAuth,
} from "./codec.ts";
import {
  type RetryPolicy,
  type RngState,
  backoffMs,
  defaultRetryPolicy,
  seedFromAddr,
} from "./retry.ts";

export class KayaError extends Error {
  readonly status?: number;
  constructor(message: string, status?: number) {
    super(message);
    this.name = "KayaError";
    this.status = status;
  }
}

export class InvalidArgument extends KayaError {
  constructor(message: string) {
    super(message, STATUS_INVALID_ARGUMENT);
    this.name = "InvalidArgument";
  }
}

export class NotFound extends KayaError {
  constructor(message: string) {
    super(message, STATUS_NOT_FOUND);
    this.name = "NotFound";
  }
}

export class TxnConflict extends KayaError {
  constructor(message: string = "transaction conflict") {
    super(message, STATUS_TXN_CONFLICT);
    this.name = "TxnConflict";
  }
}

export type KayaClientOptions = {
  /** host:port, default 127.0.0.1:7379 */
  addr?: string;
  clientToken?: string;
  maxRedirects?: number;
  /**
   * Per-attempt timeout in milliseconds. When set, also overrides
   * `retryPolicy.requestTimeoutMs`. Default comes from the retry policy (5s).
   */
  timeoutMs?: number;
  /** Transport retry policy. Leader redirects do not consume an attempt. */
  retryPolicy?: RetryPolicy;
};

type LocalWrite = { value: Buffer | null; deleted: boolean };

function parseAddr(addr: string): { host: string; port: number } {
  const idx = addr.lastIndexOf(":");
  if (idx <= 0) {
    throw new Error(`invalid address (expected host:port): ${addr}`);
  }
  const host = addr.slice(0, idx);
  const port = Number(addr.slice(idx + 1));
  if (!host || !Number.isFinite(port) || port <= 0) {
    throw new Error(`invalid address (expected host:port): ${addr}`);
  }
  return { host, port };
}

function statusError(status: number, body: Buffer): KayaError {
  const msg = body.length ? decodeErrorPayload(body) : `status ${status}`;
  if (status === STATUS_INVALID_ARGUMENT) {
    return new InvalidArgument(msg);
  }
  if (status === STATUS_NOT_FOUND) {
    return new NotFound(msg);
  }
  if (status === STATUS_TXN_CONFLICT) {
    return new TxnConflict(msg || "transaction conflict");
  }
  return new KayaError(`status ${status}: ${msg}`, status);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function copyBuf(b: Buffer): Buffer {
  return Buffer.from(b);
}

/** Buffered TCP connection with exact-length reads. */
class Conn {
  sock: net.Socket;
  private buf = Buffer.alloc(0);
  private waiters: Array<{
    n: number;
    resolve: (b: Buffer) => void;
    reject: (e: Error) => void;
  }> = [];
  private closed = false;
  private closeError: Error | null = null;

  constructor(sock: net.Socket) {
    this.sock = sock;
    sock.on("data", (chunk: Buffer) => {
      this.buf = Buffer.concat([this.buf, chunk]);
      this.pump();
    });
    sock.on("error", (err) => this.fail(new KayaError(`socket error: ${err.message}`)));
    sock.on("close", () => this.fail(new KayaError("connection closed by server")));
  }

  private pump(): void {
    while (this.waiters.length > 0 && this.buf.length >= this.waiters[0].n) {
      const w = this.waiters.shift()!;
      const out = this.buf.subarray(0, w.n);
      this.buf = this.buf.subarray(w.n);
      w.resolve(out);
    }
  }

  private fail(err: Error): void {
    if (this.closed) return;
    this.closed = true;
    this.closeError = err;
    const pending = this.waiters.splice(0);
    for (const w of pending) w.reject(err);
  }

  readExact(n: number, timeoutMs: number): Promise<Buffer> {
    if (this.closed) {
      return Promise.reject(this.closeError ?? new KayaError("connection closed"));
    }
    if (this.buf.length >= n) {
      const out = this.buf.subarray(0, n);
      this.buf = this.buf.subarray(n);
      return Promise.resolve(out);
    }
    return new Promise((resolve, reject) => {
      const timer =
        timeoutMs > 0
          ? setTimeout(() => {
              const idx = this.waiters.findIndex((w) => w.resolve === resolveWrapped);
              if (idx >= 0) this.waiters.splice(idx, 1);
              reject(new KayaError("read timeout"));
            }, timeoutMs)
          : null;

      const resolveWrapped = (b: Buffer) => {
        if (timer) clearTimeout(timer);
        resolve(b);
      };
      const rejectWrapped = (e: Error) => {
        if (timer) clearTimeout(timer);
        reject(e);
      };
      this.waiters.push({ n, resolve: resolveWrapped, reject: rejectWrapped });
    });
  }

  write(data: Buffer, timeoutMs: number): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.closed) {
        reject(this.closeError ?? new KayaError("connection closed"));
        return;
      }
      const timer =
        timeoutMs > 0
          ? setTimeout(() => reject(new KayaError("write timeout")), timeoutMs)
          : null;
      this.sock.write(data, (err) => {
        if (timer) clearTimeout(timer);
        if (err) reject(new KayaError(`write failed: ${err.message}`));
        else resolve();
      });
    });
  }

  destroy(): void {
    this.fail(new KayaError("connection closed"));
    this.sock.destroy();
  }
}

export class KayaClient {
  private host: string;
  private port: number;
  private clientToken?: string;
  private maxRedirects: number;
  private timeoutMs: number;
  private retry: RetryPolicy;
  private rng: RngState;
  private conn: Conn | null = null;

  constructor(options: KayaClientOptions | string = {}) {
    const opts: KayaClientOptions =
      typeof options === "string" ? { addr: options } : options;
    const addr = opts.addr ?? "127.0.0.1:7379";
    const { host, port } = parseAddr(addr);
    this.host = host;
    this.port = port;
    this.clientToken = opts.clientToken;
    this.maxRedirects = opts.maxRedirects ?? 3;
    this.retry = { ...(opts.retryPolicy ?? defaultRetryPolicy()) };
    if (this.retry.maxAttempts < 1) this.retry.maxAttempts = 1;
    if (opts.timeoutMs !== undefined) {
      this.timeoutMs = opts.timeoutMs;
      this.retry.requestTimeoutMs = opts.timeoutMs;
    } else {
      this.timeoutMs = this.retry.requestTimeoutMs > 0 ? this.retry.requestTimeoutMs : 5_000;
    }
    this.rng = { seed: seedFromAddr(addr) };
  }

  get addr(): string {
    return `${this.host}:${this.port}`;
  }

  /** Current retry policy (copy). */
  retryPolicy(): RetryPolicy {
    return { ...this.retry };
  }

  /**
   * Replace the transport retry policy. Leader redirects keep their own budget
   * via `maxRedirects`. `maxAttempts` is clamped to at least 1.
   */
  setRetryPolicy(policy: RetryPolicy): void {
    this.retry = { ...policy };
    if (this.retry.maxAttempts < 1) this.retry.maxAttempts = 1;
  }

  close(): void {
    if (this.conn) {
      this.conn.destroy();
      this.conn = null;
    }
  }

  private attemptTimeoutMs(): number {
    if (this.retry.requestTimeoutMs > 0) {
      return this.retry.requestTimeoutMs;
    }
    return this.timeoutMs;
  }

  private connect(): Promise<Conn> {
    if (this.conn && !this.conn.sock.destroyed) {
      return Promise.resolve(this.conn);
    }
    const timeoutMs = this.attemptTimeoutMs();
    return new Promise((resolve, reject) => {
      const sock = net.createConnection({ host: this.host, port: this.port });
      const timer =
        timeoutMs > 0
          ? setTimeout(() => {
              sock.destroy();
              reject(new KayaError("connection timeout"));
            }, timeoutMs)
          : null;
      sock.once("error", (err) => {
        if (timer) clearTimeout(timer);
        reject(new KayaError(`connection failed: ${err.message}`));
      });
      sock.once("connect", () => {
        if (timer) clearTimeout(timer);
        sock.setNoDelay(true);
        const c = new Conn(sock);
        this.conn = c;
        resolve(c);
      });
    });
  }

  private async requestOnce(
    opcode: number,
    payload: Buffer,
  ): Promise<{ status: number; body: Buffer }> {
    const timeoutMs = this.attemptTimeoutMs();
    const conn = await this.connect();
    await conn.write(encodeClientFrame(opcode, payload), timeoutMs);
    const lenBuf = await conn.readExact(4, timeoutMs);
    const respLen = lenBuf.readUInt32LE(0);
    if (respLen < 2) {
      throw new KayaError("response frame too short");
    }
    const rest = await conn.readExact(respLen, timeoutMs);
    const status = rest.readUInt16LE(0);
    const body = rest.subarray(2);
    return { status, body };
  }

  private wirePayload(opcode: number, payload: Buffer): Buffer {
    switch (opcode) {
      case OP_PUT:
      case OP_GET:
      case OP_DELETE:
      case OP_TXN_BEGIN:
      case OP_TXN_OP:
      case OP_TXN_COMMIT:
      case OP_TXN_ROLLBACK:
        return wrapClientAuth(payload, this.clientToken);
      default:
        return payload;
    }
  }

  /** @internal Wire roundtrip: retries transport errors, follows leader redirects. */
  async roundtrip(
    opcode: number,
    payload: Buffer,
  ): Promise<{ status: number; body: Buffer }> {
    const wire = this.wirePayload(opcode, payload);
    const maxAttempts = Math.max(1, this.retry.maxAttempts);
    let transportAttempts = 0;
    let redirects = 0;

    while (true) {
      try {
        const { status, body } = await this.requestOnce(opcode, wire);
        if (status === STATUS_NOT_LEADER) {
          this.close();
          if (redirects >= this.maxRedirects) {
            return { status, body };
          }
          const hint = body.toString("utf8").trim();
          if (hint) {
            const next = parseAddr(hint);
            this.host = next.host;
            this.port = next.port;
          }
          const wait = backoffMs(this.retry, redirects, this.rng);
          redirects += 1;
          if (wait > 0) await sleep(wait);
          continue;
        }
        return { status, body };
      } catch (err) {
        this.close();
        transportAttempts += 1;
        if (transportAttempts >= maxAttempts) {
          throw err;
        }
        const wait = backoffMs(this.retry, transportAttempts - 1, this.rng);
        if (wait > 0) await sleep(wait);
      }
    }
  }

  async hello(): Promise<number> {
    const { status, body } = await this.roundtrip(OP_HELLO, encodeHelloRequest());
    if (status === STATUS_OK) {
      return decodeHelloResponse(body);
    }
    throw statusError(status, body);
  }

  async put(key: Buffer, value: Buffer): Promise<void> {
    const { status, body } = await this.roundtrip(
      OP_PUT,
      encodePutPayload(key, value),
    );
    if (status !== STATUS_OK) {
      throw statusError(status, body);
    }
  }

  async get(key: Buffer): Promise<Buffer | null> {
    const { status, body } = await this.roundtrip(OP_GET, encodeKeyPayload(key));
    if (status === STATUS_OK) {
      return decodeValuePayload(body);
    }
    if (status === STATUS_NOT_FOUND) {
      return null;
    }
    throw statusError(status, body);
  }

  async delete(key: Buffer): Promise<void> {
    const { status, body } = await this.roundtrip(OP_DELETE, encodeKeyPayload(key));
    if (status !== STATUS_OK) {
      throw statusError(status, body);
    }
  }

  async health(): Promise<string> {
    const { status, body } = await this.roundtrip(OP_HEALTH, Buffer.alloc(0));
    if (status === STATUS_OK) {
      return body.toString("utf8");
    }
    throw statusError(status, body);
  }

  /** Start a Snapshot Isolation transaction on the leader. */
  async beginTxn(): Promise<Transaction> {
    const { status, body } = await this.roundtrip(OP_TXN_BEGIN, Buffer.alloc(0));
    if (status === STATUS_OK) {
      const { txnId, snapshotTs } = decodeTxnBeginResponse(body);
      return new Transaction(this, txnId, snapshotTs);
    }
    throw statusError(status, body);
  }
}

/**
 * Snapshot Isolation handle from {@link KayaClient.beginTxn}.
 *
 * Writes are staged as intents on the leader; `commit` materializes them and
 * `rollback` discards them. A local write buffer provides client-side
 * read-your-writes. Cross-range (multi-group) commits are handled by the
 * server via 2PC; the client wire path is unchanged.
 */
export class Transaction {
  private readonly client: KayaClient;
  readonly txnId: bigint;
  readonly snapshotTs: bigint;
  private local = new Map<string, LocalWrite>();
  private done = false;

  constructor(client: KayaClient, txnId: bigint, snapshotTs: bigint) {
    this.client = client;
    this.txnId = txnId;
    this.snapshotTs = snapshotTs;
  }

  private ensureOpen(): void {
    if (this.done) {
      throw new InvalidArgument("transaction already finished");
    }
  }

  /**
   * Read `key` under the transaction snapshot, with local read-your-writes.
   * Returns `null` when the key is absent (or deleted in this txn).
   */
  async get(key: Buffer): Promise<Buffer | null> {
    this.ensureOpen();
    const hit = this.local.get(key.toString("binary"));
    if (hit) {
      if (hit.deleted) return null;
      return hit.value ? copyBuf(hit.value) : null;
    }
    const payload = encodeTxnOpPayload(this.txnId, TXN_OP_GET, key);
    const { status, body } = await this.client.roundtrip(OP_TXN_OP, payload);
    if (status === STATUS_OK) {
      return decodeValuePayload(body);
    }
    if (status === STATUS_NOT_FOUND) {
      return null;
    }
    throw statusError(status, body);
  }

  /** Stage a put intent (write-write conflicts may fail immediately). */
  async put(key: Buffer, value: Buffer): Promise<void> {
    this.ensureOpen();
    const payload = encodeTxnOpPayload(this.txnId, TXN_OP_PUT, key, value);
    const { status, body } = await this.client.roundtrip(OP_TXN_OP, payload);
    if (status === STATUS_OK) {
      this.local.set(key.toString("binary"), { value: copyBuf(value), deleted: false });
      return;
    }
    throw statusError(status, body);
  }

  /** Stage a delete intent. */
  async delete(key: Buffer): Promise<void> {
    this.ensureOpen();
    const payload = encodeTxnOpPayload(this.txnId, TXN_OP_DELETE, key);
    const { status, body } = await this.client.roundtrip(OP_TXN_OP, payload);
    if (status === STATUS_OK) {
      this.local.set(key.toString("binary"), { value: null, deleted: true });
      return;
    }
    throw statusError(status, body);
  }

  /**
   * Materialize staged intents. Returns the commit timestamp on success.
   * The transaction is marked done even on failure so it cannot be reused.
   */
  async commit(): Promise<bigint> {
    this.ensureOpen();
    this.done = true;
    const { status, body } = await this.client.roundtrip(
      OP_TXN_COMMIT,
      encodeTxnIdPayload(this.txnId),
    );
    if (status === STATUS_OK) {
      return decodeTxnCommitResponse(body);
    }
    throw statusError(status, body);
  }

  /** Discard staged intents without committing. */
  async rollback(): Promise<void> {
    this.ensureOpen();
    this.done = true;
    const { status, body } = await this.client.roundtrip(
      OP_TXN_ROLLBACK,
      encodeTxnIdPayload(this.txnId),
    );
    if (status === STATUS_OK) {
      return;
    }
    throw statusError(status, body);
  }
}
