import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as net from "node:net";
import { KayaClient } from "../src/client.ts";
import {
  OP_GET,
  OP_HEALTH,
  OP_HELLO,
  OP_PUT,
  STATUS_NOT_FOUND,
  STATUS_NOT_LEADER,
  STATUS_OK,
  CLIENT_AUTH_PREFIX,
} from "../src/codec.ts";

/** Accumulate-and-read helper for the mock server. */
class SockBuf {
  private buf = Buffer.alloc(0);
  private waiters: Array<{ n: number; resolve: (b: Buffer) => void; reject: (e: Error) => void }> =
    [];
  private closed = false;

  constructor(sock: net.Socket) {
    sock.on("data", (c) => {
      this.buf = Buffer.concat([this.buf, c]);
      this.pump();
    });
    sock.on("error", () => this.fail());
    sock.on("close", () => this.fail());
  }

  private pump(): void {
    while (this.waiters.length && this.buf.length >= this.waiters[0].n) {
      const w = this.waiters.shift()!;
      const out = this.buf.subarray(0, w.n);
      this.buf = this.buf.subarray(w.n);
      w.resolve(out);
    }
  }

  private fail(): void {
    if (this.closed) return;
    this.closed = true;
    for (const w of this.waiters.splice(0)) {
      w.reject(new Error("closed"));
    }
  }

  readExact(n: number): Promise<Buffer> {
    if (this.buf.length >= n) {
      const out = this.buf.subarray(0, n);
      this.buf = this.buf.subarray(n);
      return Promise.resolve(out);
    }
    return new Promise((resolve, reject) => {
      this.waiters.push({ n, resolve, reject });
    });
  }
}

function sendResponse(sock: net.Socket, status: number, payload: Buffer = Buffer.alloc(0)) {
  const frameLen = 2 + payload.length;
  const out = Buffer.allocUnsafe(4 + frameLen);
  out.writeUInt32LE(frameLen, 0);
  out.writeUInt16LE(status, 4);
  payload.copy(out, 6);
  sock.write(out);
}

function stripAuth(payload: Buffer): Buffer {
  if (
    payload.length >= CLIENT_AUTH_PREFIX.length &&
    payload.subarray(0, CLIENT_AUTH_PREFIX.length).equals(CLIENT_AUTH_PREFIX)
  ) {
    const tlen = payload.readUInt16LE(CLIENT_AUTH_PREFIX.length);
    return payload.subarray(CLIENT_AUTH_PREFIX.length + 2 + tlen);
  }
  return payload;
}

async function withServer(
  handler: (sock: net.Socket, opcode: number, payload: Buffer) => boolean | Promise<boolean>,
  fn: (port: number) => Promise<void>,
): Promise<void> {
  const server = net.createServer((sock) => {
    const sb = new SockBuf(sock);
    (async () => {
      try {
        while (true) {
          const lenBuf = await sb.readExact(4);
          const frameLen = lenBuf.readUInt32LE(0);
          const body = await sb.readExact(frameLen);
          const opcode = body.readUInt8(0);
          const payload = body.subarray(1);
          const cont = await handler(sock, opcode, payload);
          if (!cont) break;
        }
      } catch {
        // client closed
      } finally {
        sock.destroy();
      }
    })();
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const addr = server.address();
  if (!addr || typeof addr === "string") throw new Error("no port");
  try {
    await fn(addr.port);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe("client loopback", () => {
  it("put/get/hello/health over mock server", async () => {
    const store = new Map<string, Buffer>();
    await withServer(async (sock, opcode, payload) => {
      if (opcode === OP_HELLO) {
        const ver = Buffer.allocUnsafe(2);
        ver.writeUInt16LE(1, 0);
        sendResponse(sock, STATUS_OK, ver);
      } else if (opcode === OP_PUT) {
        const p = stripAuth(payload);
        const klen = p.readUInt32LE(0);
        const vlen = p.readUInt32LE(4);
        const key = p.subarray(8, 8 + klen);
        const value = p.subarray(8 + klen, 8 + klen + vlen);
        store.set(key.toString("binary"), Buffer.from(value));
        sendResponse(sock, STATUS_OK);
      } else if (opcode === OP_GET) {
        const p = stripAuth(payload);
        const klen = p.readUInt32LE(0);
        const key = p.subarray(4, 4 + klen);
        const v = store.get(key.toString("binary"));
        if (v) {
          const body = Buffer.allocUnsafe(4 + v.length);
          body.writeUInt32LE(v.length, 0);
          v.copy(body, 4);
          sendResponse(sock, STATUS_OK, body);
        } else {
          sendResponse(sock, STATUS_NOT_FOUND);
        }
      } else if (opcode === OP_HEALTH) {
        sendResponse(sock, STATUS_OK, Buffer.from("leader"));
      } else {
        sendResponse(sock, 9);
      }
      return true;
    }, async (port) => {
      const client = new KayaClient({ addr: `127.0.0.1:${port}`, timeoutMs: 2000 });
      try {
        assert.equal(await client.hello(), 1);
        await client.put(Buffer.from("k1"), Buffer.from("v1"));
        assert.equal((await client.get(Buffer.from("k1")))?.toString(), "v1");
        assert.equal(await client.get(Buffer.from("missing")), null);
        assert.equal(await client.health(), "leader");
      } finally {
        client.close();
      }
    });
  });

  it("follows NOT_LEADER redirect", async () => {
    let leaderPort = 0;
    const leaderStore = new Map([
      [Buffer.from("k").toString("binary"), Buffer.from("leader-value")],
    ]);

    await withServer(async (sock, opcode, payload) => {
      if (opcode === OP_GET) {
        const p = stripAuth(payload);
        const klen = p.readUInt32LE(0);
        const key = p.subarray(4, 4 + klen);
        const v = leaderStore.get(key.toString("binary"));
        if (v) {
          const body = Buffer.allocUnsafe(4 + v.length);
          body.writeUInt32LE(v.length, 0);
          v.copy(body, 4);
          sendResponse(sock, STATUS_OK, body);
        } else {
          sendResponse(sock, STATUS_NOT_FOUND);
        }
      }
      return true;
    }, async (lport) => {
      leaderPort = lport;
      await withServer(async (sock, opcode) => {
        if (opcode === OP_GET) {
          sendResponse(sock, STATUS_NOT_LEADER, Buffer.from(`127.0.0.1:${leaderPort}`));
        }
        return true;
      }, async (fport) => {
        const client = new KayaClient({ addr: `127.0.0.1:${fport}`, timeoutMs: 2000 });
        try {
          const v = await client.get(Buffer.from("k"));
          assert.equal(v?.toString(), "leader-value");
        } finally {
          client.close();
        }
      });
    });
  });
});
