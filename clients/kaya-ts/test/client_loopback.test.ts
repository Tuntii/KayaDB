import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as net from "node:net";
import { KayaClient, TxnConflict } from "../src/client.ts";
import {
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
  CLIENT_AUTH_PREFIX,
  decodeTxnIdPayload,
  decodeTxnOpPayload,
  encodeTxnBeginResponse,
  encodeTxnCommitResponse,
} from "../src/codec.ts";
import { retryPolicyNone } from "../src/retry.ts";

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

  it("leader redirect does not consume a retry attempt", async () => {
    let leaderPort = 0;
    await withServer(async (sock, opcode) => {
      if (opcode === OP_HEALTH) {
        sendResponse(sock, STATUS_OK, Buffer.from("leader"));
      }
      return true;
    }, async (lport) => {
      leaderPort = lport;
      await withServer(async (sock, opcode) => {
        if (opcode === OP_HEALTH) {
          sendResponse(sock, STATUS_NOT_LEADER, Buffer.from(`127.0.0.1:${leaderPort}`));
        }
        return true;
      }, async (fport) => {
        const client = new KayaClient({
          addr: `127.0.0.1:${fport}`,
          timeoutMs: 2000,
          retryPolicy: retryPolicyNone(),
        });
        try {
          assert.equal(await client.health(), "leader");
        } finally {
          client.close();
        }
      });
    });
  });

  it("retries a transport error then succeeds", async () => {
    let hits = 0;
    await withServer(async (sock, opcode) => {
      hits += 1;
      if (hits === 1) {
        sock.destroy();
        return false;
      }
      if (opcode === OP_HEALTH) {
        sendResponse(sock, STATUS_OK, Buffer.from("leader"));
      }
      return true;
    }, async (port) => {
      const client = new KayaClient({
        addr: `127.0.0.1:${port}`,
        timeoutMs: 2000,
        retryPolicy: {
          maxAttempts: 3,
          baseBackoffMs: 1,
          maxBackoffMs: 5,
          jitter: false,
          requestTimeoutMs: 2000,
        },
      });
      try {
        assert.equal(await client.health(), "leader");
        assert.ok(hits >= 2);
      } finally {
        client.close();
      }
    });
  });
});

describe("txn loopback", () => {
  type MockTxn = {
    snapshotTs: bigint;
    writes: Map<string, Buffer | null>;
    committed: boolean;
    aborted: boolean;
  };

  function encodeValue(v: Buffer): Buffer {
    const body = Buffer.allocUnsafe(4 + v.length);
    body.writeUInt32LE(v.length, 0);
    v.copy(body, 4);
    return body;
  }

  async function withTxnServer(fn: (port: number) => Promise<void>): Promise<void> {
    const txns = new Map<bigint, MockTxn>();
    let nextId = 1n;
    await withServer(async (sock, opcode, payload) => {
      const p = stripAuth(payload);
      if (opcode === OP_TXN_BEGIN) {
        const id = nextId;
        nextId += 1n;
        txns.set(id, {
          snapshotTs: id * 10n,
          writes: new Map(),
          committed: false,
          aborted: false,
        });
        sendResponse(sock, STATUS_OK, encodeTxnBeginResponse(id, id * 10n));
      } else if (opcode === OP_TXN_OP) {
        let decoded;
        try {
          decoded = decodeTxnOpPayload(p);
        } catch (err) {
          sendResponse(sock, STATUS_INVALID_ARGUMENT, Buffer.from(String(err)));
          return true;
        }
        const txn = txns.get(decoded.txnId);
        if (!txn || txn.committed || txn.aborted) {
          sendResponse(sock, STATUS_INVALID_ARGUMENT, Buffer.from("unknown txn"));
          return true;
        }
        const k = decoded.key.toString("binary");
        if (decoded.op === TXN_OP_GET) {
          if (txn.writes.has(k)) {
            const v = txn.writes.get(k);
            if (v === null) sendResponse(sock, STATUS_NOT_FOUND);
            else sendResponse(sock, STATUS_OK, encodeValue(v!));
          } else {
            sendResponse(sock, STATUS_NOT_FOUND);
          }
        } else if (decoded.op === TXN_OP_PUT) {
          txn.writes.set(k, Buffer.from(decoded.value ?? Buffer.alloc(0)));
          sendResponse(sock, STATUS_OK);
        } else if (decoded.op === TXN_OP_DELETE) {
          txn.writes.set(k, null);
          sendResponse(sock, STATUS_OK);
        } else {
          sendResponse(sock, STATUS_INVALID_ARGUMENT, Buffer.from("bad op"));
        }
      } else if (opcode === OP_TXN_COMMIT) {
        const txnId = decodeTxnIdPayload(p);
        const txn = txns.get(txnId);
        if (!txn || txn.committed || txn.aborted) {
          sendResponse(sock, STATUS_INVALID_ARGUMENT, Buffer.from("unknown txn"));
        } else if (txn.writes.has("conflict")) {
          sendResponse(sock, STATUS_TXN_CONFLICT, Buffer.from("txn conflict"));
        } else {
          txn.committed = true;
          sendResponse(sock, STATUS_OK, encodeTxnCommitResponse(txn.snapshotTs + 1n));
        }
      } else if (opcode === OP_TXN_ROLLBACK) {
        const txnId = decodeTxnIdPayload(p);
        const txn = txns.get(txnId);
        if (!txn) {
          sendResponse(sock, STATUS_INVALID_ARGUMENT, Buffer.from("unknown txn"));
        } else {
          txn.aborted = true;
          sendResponse(sock, STATUS_OK);
        }
      } else if (opcode === OP_HEALTH) {
        sendResponse(sock, STATUS_OK, Buffer.from("leader"));
      } else {
        sendResponse(sock, 9);
      }
      return true;
    }, fn);
  }

  it("begin put get commit, then refuse reuse", async () => {
    await withTxnServer(async (port) => {
      const client = new KayaClient({
        addr: `127.0.0.1:${port}`,
        timeoutMs: 2000,
        retryPolicy: retryPolicyNone(),
      });
      try {
        const txn = await client.beginTxn();
        assert.notEqual(txn.txnId, 0n);
        assert.equal(txn.snapshotTs, txn.txnId * 10n);

        await txn.put(Buffer.from("a"), Buffer.from("1"));
        assert.equal((await txn.get(Buffer.from("a")))?.toString(), "1");

        await txn.put(Buffer.from("b"), Buffer.from("2"));
        const ts = await txn.commit();
        assert.notEqual(ts, 0n);

        await assert.rejects(() => txn.put(Buffer.from("c"), Buffer.from("3")));
      } finally {
        client.close();
      }
    });
  });

  it("rollback after local delete", async () => {
    await withTxnServer(async (port) => {
      const client = new KayaClient({
        addr: `127.0.0.1:${port}`,
        timeoutMs: 2000,
        retryPolicy: retryPolicyNone(),
      });
      try {
        const txn = await client.beginTxn();
        await txn.put(Buffer.from("x"), Buffer.from("y"));
        await txn.delete(Buffer.from("x"));
        assert.equal(await txn.get(Buffer.from("x")), null);
        await txn.rollback();
      } finally {
        client.close();
      }
    });
  });

  it("commit of key 'conflict' returns TxnConflict", async () => {
    await withTxnServer(async (port) => {
      const client = new KayaClient({
        addr: `127.0.0.1:${port}`,
        timeoutMs: 2000,
        retryPolicy: retryPolicyNone(),
      });
      try {
        const txn = await client.beginTxn();
        await txn.put(Buffer.from("conflict"), Buffer.from("v"));
        await assert.rejects(() => txn.commit(), (err: unknown) => err instanceof TxnConflict);
      } finally {
        client.close();
      }
    });
  });

  it("server-side get miss returns null", async () => {
    await withTxnServer(async (port) => {
      const client = new KayaClient({
        addr: `127.0.0.1:${port}`,
        timeoutMs: 2000,
        retryPolicy: retryPolicyNone(),
      });
      try {
        const txn = await client.beginTxn();
        assert.equal(await txn.get(Buffer.from("missing")), null);
        await txn.rollback();
      } finally {
        client.close();
      }
    });
  });

  it("wraps TXN_BEGIN with client token framing", async () => {
    let sawAuth = false;
    await withServer(async (sock, opcode, payload) => {
      if (opcode === OP_TXN_BEGIN) {
        sawAuth =
          payload.length >= CLIENT_AUTH_PREFIX.length &&
          payload.subarray(0, CLIENT_AUTH_PREFIX.length).equals(CLIENT_AUTH_PREFIX);
        sendResponse(sock, STATUS_OK, encodeTxnBeginResponse(1n, 10n));
      }
      return true;
    }, async (port) => {
      const client = new KayaClient({
        addr: `127.0.0.1:${port}`,
        clientToken: "tok",
        timeoutMs: 2000,
        retryPolicy: retryPolicyNone(),
      });
      try {
        const txn = await client.beginTxn();
        assert.equal(txn.txnId, 1n);
        assert.ok(sawAuth);
      } finally {
        client.close();
      }
    });
  });
});
