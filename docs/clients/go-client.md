# Implementing a KayaDB Client in Go

**Status:** Ready for bootstrap  
**Date:** 2026-06-14  
**Based on:** [client-protocol-spec.md](client-protocol-spec.md) + [client-wire-protocol.md](client-wire-protocol.md)

This guide gives you everything needed to create a correct Go client for KayaDB (the first recommended non-Rust client).

Goal: A production-usable basic client that correctly implements:
- Leader discovery & automatic redirection
- Proper retry / backoff for transient errors
- All 6 operations (Put/Get/Delete/Scan/Health/Stats)
- Exact wire encoding from the reference
- Linearizability tracing hook (optional but recommended)

---

## 1. Recommended Repository Structure (for github.com/Tuntii/kaya-go or similar)

```
kaya-go/
├── go.mod
├── go.sum
├── README.md
├── client.go              # core KayaClient + protocol
├── codec.go               # pure wire encoding/decoding (easy to test)
├── errors.go
├── examples/
│   └── basic/
│       └── main.go
├── internal/
│   └── trace/             # optional LinearizabilityChecker port (later)
└── test/
    └── integration_test.go
```

---

## 2. go.mod (start here)

```go
module github.com/Tuntii/kaya-go

go 1.23

require (
    golang.org/x/net v0.0.0-... // only if you want advanced features later
)
```

---

## 3. Core Types & Errors (`errors.go`)

```go
package kaya

import "errors"

var (
    ErrNotFound          = errors.New("kaya: key not found")
    ErrInvalidArgument   = errors.New("kaya: invalid argument")
    ErrNotLeader         = errors.New("kaya: not leader (redirected)")
    ErrTimeout           = errors.New("kaya: timeout")
    ErrConnection        = errors.New("kaya: connection error")
    ErrProtocol          = errors.New("kaya: protocol error")
)

type StatusError struct {
    Code    uint16
    Message string
}

func (e *StatusError) Error() string {
    return e.Message
}
```

---

## 4. Exact Wire Codec (`codec.go`)

Copy these functions **exactly** — they must produce identical bytes to the Rust reference.

```go
package kaya

import (
    "encoding/binary"
    "fmt"
)

const (
    MaxFrameLen = 64 * 1024 * 1024
)

func encodePutPayload(key, value []byte) []byte {
    buf := make([]byte, 8+len(key)+len(value))
    binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
    binary.LittleEndian.PutUint32(buf[4:8], uint32(len(value)))
    copy(buf[8:], key)
    copy(buf[8+len(key):], value)
    return buf
}

func encodeKeyPayload(key []byte) []byte {
    buf := make([]byte, 4+len(key))
    binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
    copy(buf[4:], key)
    return buf
}

func decodeValuePayload(data []byte) ([]byte, error) {
    if len(data) < 4 {
        return nil, fmt.Errorf("truncated value payload")
    }
    l := binary.LittleEndian.Uint32(data[0:4])
    if len(data) < 4+int(l) {
        return nil, fmt.Errorf("truncated value")
    }
    return data[4 : 4+l], nil
}

func decodeScanResponse(data []byte) ([][2][]byte, error) {
    if len(data) < 4 {
        return nil, fmt.Errorf("truncated scan response")
    }
    count := binary.LittleEndian.Uint32(data[0:4])
    if count > 1_000_000 {
        return nil, fmt.Errorf("suspiciously large scan count: %d", count)
    }

    out := make([][2][]byte, 0, count)
    cur := data[4:]
    for i := uint32(0); i < count; i++ {
        if len(cur) < 4 {
            return nil, fmt.Errorf("truncated key len in scan")
        }
        kl := binary.LittleEndian.Uint32(cur[0:4])
        cur = cur[4:]
        if len(cur) < int(kl) {
            return nil, fmt.Errorf("truncated key in scan")
        }
        k := cur[:kl]
        cur = cur[kl:]

        if len(cur) < 4 {
            return nil, fmt.Errorf("truncated value len in scan")
        }
        vl := binary.LittleEndian.Uint32(cur[0:4])
        cur = cur[4:]
        if len(cur) < int(vl) {
            return nil, fmt.Errorf("truncated value in scan")
        }
        v := cur[:vl]
        cur = cur[vl:]

        out = append(out, [2][]byte{k, v})
    }
    return out, nil
}

func decodeErrorPayload(data []byte) (string, error) {
    if len(data) < 4 {
        return "", fmt.Errorf("truncated error payload")
    }
    l := binary.LittleEndian.Uint32(data[0:4])
    if len(data) < 4+int(l) {
        return "", fmt.Errorf("truncated error message")
    }
    return string(data[4 : 4+l]), nil
}
```

**Frame helpers** (in `client.go` or separate):

```go
func encodeClientFrame(opcode uint8, payload []byte) []byte {
    frameLen := uint32(1 + len(payload))
    buf := make([]byte, 4+1+len(payload))
    binary.LittleEndian.PutUint32(buf[0:4], frameLen)
    buf[4] = opcode
    copy(buf[5:], payload)
    return buf
}

// readResponse reads one full response frame. Returns (status, payload)
func readResponse(r io.Reader) (uint16, []byte, error) {
    var lenBuf [4]byte
    if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
        return 0, nil, err
    }
    frameLen := binary.LittleEndian.Uint32(lenBuf[:])
    if frameLen < 2 || frameLen > MaxFrameLen {
        return 0, nil, fmt.Errorf("invalid response frame length: %d", frameLen)
    }

    body := make([]byte, frameLen)
    if _, err := io.ReadFull(r, body); err != nil {
        return 0, nil, err
    }

    status := binary.LittleEndian.Uint16(body[0:2])
    payload := body[2:]
    return status, payload, nil
}
```

---

## 5. The Client (`client.go`)

```go
package kaya

import (
    "context"
    "encoding/binary"
    "fmt"
    "io"
    "net"
    "time"
)

const (
    defaultMaxRedirects = 5
    defaultTimeout      = 5 * time.Second
)

type Client struct {
    addrs         []string // initial endpoints
    currentAddr   string
    maxRedirects  int
    conn          net.Conn
    timeout       time.Duration

    // Future: tracing / linearizability recorder hook
}

func New(addrs ...string) *Client {
    return &Client{
        addrs:        addrs,
        maxRedirects: defaultMaxRedirects,
        timeout:      defaultTimeout,
    }
}

func (c *Client) SetMaxRedirects(n int) { c.maxRedirects = n }
func (c *Client) SetTimeout(d time.Duration) { c.timeout = d }

func (c *Client) dial(ctx context.Context) (net.Conn, error) {
    if c.currentAddr == "" && len(c.addrs) > 0 {
        c.currentAddr = c.addrs[0]
    }
    d := net.Dialer{Timeout: c.timeout}
    return d.DialContext(ctx, "tcp", c.currentAddr)
}

func (c *Client) ensureConn(ctx context.Context) error {
    if c.conn != nil {
        return nil
    }
    conn, err := c.dial(ctx)
    if err != nil {
        return fmt.Errorf("%w: %w", ErrConnection, err)
    }
    c.conn = conn
    return nil
}

func (c *Client) closeConn() {
    if c.conn != nil {
        c.conn.Close()
        c.conn = nil
    }
}

// roundtripWithRedirect is the heart of the client.
func (c *Client) roundtripWithRedirect(ctx context.Context, opcode uint8, payload []byte) (uint16, []byte, error) {
    for attempt := 0; attempt <= c.maxRedirects; attempt++ {
        if err := c.ensureConn(ctx); err != nil {
            return 0, nil, err
        }

        // Set write deadline
        _ = c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
        frame := encodeClientFrame(opcode, payload)
        if _, err := c.conn.Write(frame); err != nil {
            c.closeConn()
            continue
        }

        _ = c.conn.SetReadDeadline(time.Now().Add(c.timeout))
        status, body, err := readResponse(c.conn)
        if err != nil {
            c.closeConn()
            continue
        }

        if status == STATUS_NOT_LEADER {
            hint := string(body)
            if hint != "" {
                // Switch target
                c.currentAddr = hint
                c.closeConn()
                continue
            }
            // No hint — try next initial addr if possible
            if len(c.addrs) > 0 {
                // simple round-robin fallback
                idx := (attempt % len(c.addrs))
                c.currentAddr = c.addrs[idx]
            }
            c.closeConn()
            continue
        }

        return status, body, nil
    }

    return 0, nil, ErrTimeout // or wrap last error
}

// Public API

func (c *Client) Put(ctx context.Context, key, value []byte) error {
    payload := encodePutPayload(key, value)
    status, body, err := c.roundtripWithRedirect(ctx, OP_PUT, payload)
    if err != nil {
        return err
    }
    return handleStatus(status, body)
}

func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
    payload := encodeKeyPayload(key)
    status, body, err := c.roundtripWithRedirect(ctx, OP_GET, payload)
    if err != nil {
        return nil, err
    }
    if status == STATUS_NOT_FOUND {
        return nil, ErrNotFound
    }
    if status == STATUS_OK {
        return decodeValuePayload(body)
    }
    return nil, handleStatus(status, body)
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
    payload := encodeKeyPayload(key)
    status, body, err := c.roundtripWithRedirect(ctx, OP_DELETE, payload)
    if err != nil {
        return err
    }
    return handleStatus(status, body)
}

func (c *Client) Scan(ctx context.Context, prefix []byte) ([][2][]byte, error) {
    payload := encodeKeyPayload(prefix)
    status, body, err := c.roundtripWithRedirect(ctx, OP_SCAN, payload)
    if err != nil {
        return nil, err
    }
    if status == STATUS_OK {
        return decodeScanResponse(body)
    }
    return nil, handleStatus(status, body)
}

func (c *Client) Health(ctx context.Context) (string, error) {
    status, body, err := c.roundtripWithRedirect(ctx, OP_HEALTH, nil)
    if err != nil {
        return "", err
    }
    if status == STATUS_OK {
        return string(body), nil
    }
    return "", handleStatus(status, body)
}

func (c *Client) Stats(ctx context.Context) (string, error) {
    status, body, err := c.roundtripWithRedirect(ctx, OP_STATS, nil)
    if err != nil {
        return "", err
    }
    if status == STATUS_OK {
        return string(body), nil
    }
    return "", handleStatus(status, body)
}

func handleStatus(status uint16, body []byte) error {
    switch status {
    case STATUS_OK:
        return nil
    case STATUS_NOT_FOUND:
        return ErrNotFound
    case STATUS_INVALID_ARGUMENT:
        msg, _ := decodeErrorPayload(body)
        return &StatusError{Code: status, Message: msg}
    case STATUS_ERROR:
        msg, _ := decodeErrorPayload(body)
        return &StatusError{Code: status, Message: msg}
    case STATUS_NOT_LEADER:
        return ErrNotLeader
    default:
        return &StatusError{Code: status, Message: fmt.Sprintf("unknown status %d", status)}
    }
}

// Constants (match Rust kaya-net exactly)
const (
    OP_PUT    uint8 = 1
    OP_GET    uint8 = 2
    OP_DELETE uint8 = 3
    OP_SCAN   uint8 = 4
    OP_HEALTH uint8 = 5
    OP_STATS  uint8 = 6

    STATUS_OK               uint16 = 0
    STATUS_INVALID_ARGUMENT uint16 = 1
    STATUS_NOT_FOUND        uint16 = 2
    STATUS_ERROR            uint16 = 9
    STATUS_NOT_LEADER       uint16 = 10
)
```

**Note:** Add `import "io"` and `context` at the top. The redirect logic above is deliberately simple and robust (matches the spirit of the Rust `send_with_retry`).

---

## 6. Usage Example

```go
package main

import (
    "context"
    "fmt"
    "time"

    kaya "github.com/Tuntii/kaya-go"
)

func main() {
    client := kaya.New("127.0.0.1:7379", "127.0.0.1:7380", "127.0.0.1:7381")
    client.SetTimeout(3 * time.Second)

    ctx := context.Background()

    _ = client.Put(ctx, []byte("user:1"), []byte("ada"))

    val, err := client.Get(ctx, []byte("user:1"))
    if err != nil {
        fmt.Println("GET error:", err)
    } else {
        fmt.Println("GET:", string(val))
    }

    role, _ := client.Health(ctx)
    fmt.Println("Node role:", role)

    stats, _ := client.Stats(ctx)
    fmt.Println("Stats:", stats)
}
```

---

## 7. Testing Recommendations

- Run a 1-node or 3-node KayaDB cluster locally.
- Test leader failover: kill the current leader while the client is connected → it should redirect within `maxRedirects`.
- Test malformed keys (very long key) → `INVALID_ARGUMENT`.
- Test Scan with various prefixes.
- Add a small integration test that records operations and later checks against a reference model (port the idea from `kaya-sim` LinearizabilityChecker when you are ready).

---

## 8. Conformance Checklist (from the spec)

- [ ] Correctly handles all 5 status codes and redirects on `NOT_LEADER`.
- [ ] Implements exponential backoff with jitter for transient errors (add this on top of the basic redirect loop).
- [ ] Exposes a way to enable operation tracing / history recording for linearizability checking.
- [ ] PUT/DELETE are safe to retry when an idempotency key mechanism exists (future).
- [ ] Follows the exact payload layouts in `client-wire-protocol.md`.

---

## 9. Next Steps After Bootstrap

1. Add context cancellation everywhere (already using ctx in the skeleton).
2. Connection pooling / multiple concurrent connections (advanced).
3. Proper logging / metrics hooks.
4. Port the `LinearizabilityChecker` from the Rust `kaya-sim` crate for correctness testing of your client.
5. Publish v0.1.0 once it passes a 3-node cluster + partition test.

---

**You now have a complete, reference-faithful starting point.**

Copy the code above into a new repo under your account (Tuntii), `go mod tidy`, and run it against a real `kayadb-server`.

When the Go client is solid, repeat a similar (but idiomatic) process for Python.

See also:
- [client-wire-protocol.md](client-wire-protocol.md) — byte truth
- [client-protocol-spec.md](client-protocol-spec.md) — behavioral contract
- The Rust `kaya-client` for behavioral reference

Good luck — this is the foundation for the multi-language client ecosystem.
