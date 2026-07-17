/**
 * Async KayaDB TCP client using Node net.Socket.
 *
 * Mirrors the Python and Go clients: connection reuse, NOT_LEADER redirect,
 * optional client token, and per-request timeout.
 */

import * as net from "node:net";
import {
  OP_DELETE,
  OP_GET,
  OP_HEALTH,
  OP_HELLO,
  OP_PUT,
  STATUS_INVALID_ARGUMENT,
  STATUS_NOT_FOUND,
  STATUS_NOT_LEADER,
  STATUS_OK,
  decodeErrorPayload,
  decodeHelloResponse,
  decodeValuePayload,
  encodeClientFrame,
  encodeHelloRequest,
  encodeKeyPayload,
  encodePutPayload,
  wrapClientAuth,
} from "./codec.ts";

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

export type KayaClientOptions = {
  /** host:port, default 127.0.0.1:7379 */
  addr?: string;
  clientToken?: string;
  maxRedirects?: number;
  /** Request timeout in milliseconds; default 5000 */
  timeoutMs?: number;
};

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
  return new KayaError(`status ${status}: ${msg}`, status);
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
  private conn: Conn | null = null;

  constructor(options: KayaClientOptions | string = {}) {
    const opts: KayaClientOptions =
      typeof options === "string" ? { addr: options } : options;
    const { host, port } = parseAddr(opts.addr ?? "127.0.0.1:7379");
    this.host = host;
    this.port = port;
    this.clientToken = opts.clientToken;
    this.maxRedirects = opts.maxRedirects ?? 3;
    this.timeoutMs = opts.timeoutMs ?? 5_000;
  }

  get addr(): string {
    return `${this.host}:${this.port}`;
  }

  close(): void {
    if (this.conn) {
      this.conn.destroy();
      this.conn = null;
    }
  }

  private connect(): Promise<Conn> {
    if (this.conn && !this.conn.sock.destroyed) {
      return Promise.resolve(this.conn);
    }
    return new Promise((resolve, reject) => {
      const sock = net.createConnection({ host: this.host, port: this.port });
      const timer =
        this.timeoutMs > 0
          ? setTimeout(() => {
              sock.destroy();
              reject(new KayaError("connection timeout"));
            }, this.timeoutMs)
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
    const conn = await this.connect();
    await conn.write(encodeClientFrame(opcode, payload), this.timeoutMs);
    const lenBuf = await conn.readExact(4, this.timeoutMs);
    const respLen = lenBuf.readUInt32LE(0);
    if (respLen < 2) {
      throw new KayaError("response frame too short");
    }
    const rest = await conn.readExact(respLen, this.timeoutMs);
    const status = rest.readUInt16LE(0);
    const body = rest.subarray(2);
    return { status, body };
  }

  private async send(
    opcode: number,
    payload: Buffer,
  ): Promise<{ status: number; body: Buffer }> {
    // Data-path ops carry the optional token (matches Python/Go clients).
    let wire = payload;
    if (opcode === OP_PUT || opcode === OP_GET || opcode === OP_DELETE) {
      wire = wrapClientAuth(payload, this.clientToken);
    }

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
          redirects += 1;
          continue;
        }
        return { status, body };
      } catch (err) {
        this.close();
        if (redirects >= this.maxRedirects) {
          throw err;
        }
        redirects += 1;
      }
    }
  }

  async hello(): Promise<number> {
    const { status, body } = await this.send(OP_HELLO, encodeHelloRequest());
    if (status === STATUS_OK) {
      return decodeHelloResponse(body);
    }
    throw statusError(status, body);
  }

  async put(key: Buffer, value: Buffer): Promise<void> {
    const { status, body } = await this.send(
      OP_PUT,
      encodePutPayload(key, value),
    );
    if (status !== STATUS_OK) {
      throw statusError(status, body);
    }
  }

  async get(key: Buffer): Promise<Buffer | null> {
    const { status, body } = await this.send(OP_GET, encodeKeyPayload(key));
    if (status === STATUS_OK) {
      return decodeValuePayload(body);
    }
    if (status === STATUS_NOT_FOUND) {
      return null;
    }
    throw statusError(status, body);
  }

  async delete(key: Buffer): Promise<void> {
    const { status, body } = await this.send(OP_DELETE, encodeKeyPayload(key));
    if (status !== STATUS_OK) {
      throw statusError(status, body);
    }
  }

  async health(): Promise<string> {
    const { status, body } = await this.send(OP_HEALTH, Buffer.alloc(0));
    if (status === STATUS_OK) {
      return body.toString("utf8");
    }
    throw statusError(status, body);
  }
}
