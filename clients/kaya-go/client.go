package kaya

import (
	"context"
	"fmt"
	"net"
	"time"
)

const (
	defaultMaxRedirects = 3
	defaultTimeout      = 5 * time.Second
)

// KayaClient is a TCP client for the KayaDB wire protocol.
type KayaClient struct {
	addr          string
	maxRedirects  int
	timeout       time.Duration
	conn          net.Conn
	clientToken   *string
}

// Connect creates a client targeting addr (host:port). The TCP connection is
// opened lazily on the first operation.
func Connect(addr string) (*KayaClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("%w: empty address", ErrInvalidArgument)
	}
	return &KayaClient{
		addr:         addr,
		maxRedirects: defaultMaxRedirects,
		timeout:      defaultTimeout,
	}, nil
}

// SetClientToken sets the token sent with data-path operations (PUT/GET/DELETE/SCAN/STATS).
func (c *KayaClient) SetClientToken(token string) {
	if token == "" {
		c.clientToken = nil
		return
	}
	c.clientToken = &token
}

// SetTimeout configures per-operation read/write deadlines.
func (c *KayaClient) SetTimeout(d time.Duration) {
	c.timeout = d
}

// SetMaxRedirects configures how many NOT_LEADER redirects to follow.
func (c *KayaClient) SetMaxRedirects(n int) {
	c.maxRedirects = n
}

// Addr returns the current target address (updated after leader redirects).
func (c *KayaClient) Addr() string {
	return c.addr
}

// Close closes the underlying TCP connection, if any.
func (c *KayaClient) Close() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *KayaClient) wirePayload(opcode uint8, payload []byte) []byte {
	switch opcode {
	case OpPut, OpGet, OpDelete, OpScan, OpStats:
		return encodeClientAuthPayload(payload, c.clientToken)
	default:
		return payload
	}
}

func (c *KayaClient) dial(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: c.timeout}
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConnection, err)
	}
	return conn, nil
}

func (c *KayaClient) ensureConn(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	conn, err := c.dial(ctx)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *KayaClient) closeConn() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *KayaClient) roundtripWithRedirect(ctx context.Context, opcode uint8, payload []byte) (uint16, []byte, error) {
	wirePayload := c.wirePayload(opcode, payload)
	var lastErr error

	for attempt := 0; attempt <= c.maxRedirects; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}

		if err := c.ensureConn(ctx); err != nil {
			lastErr = err
			continue
		}

		deadline := time.Now().Add(c.timeout)
		_ = c.conn.SetWriteDeadline(deadline)
		frame := encodeClientFrame(opcode, wirePayload)
		if _, err := c.conn.Write(frame); err != nil {
			c.closeConn()
			lastErr = fmt.Errorf("%w: %v", ErrConnection, err)
			continue
		}

		_ = c.conn.SetReadDeadline(deadline)
		status, body, err := readResponse(c.conn)
		if err != nil {
			c.closeConn()
			lastErr = err
			continue
		}

		if status == StatusNotLeader {
			hint := string(body)
			if hint != "" {
				c.addr = hint
				c.closeConn()
				continue
			}
			c.closeConn()
			lastErr = ErrNotLeader
			continue
		}

		return status, body, nil
	}

	if lastErr != nil {
		return 0, nil, lastErr
	}
	return 0, nil, ErrTimeout
}

func handleStatus(status uint16, body []byte) error {
	switch status {
	case StatusOK:
		return nil
	case StatusNotFound:
		return ErrNotFound
	case StatusInvalidArgument:
		msg, _ := decodeErrorPayload(body)
		if msg == "" {
			msg = "invalid argument"
		}
		return &StatusError{Code: status, Message: msg}
	case StatusServerError:
		msg, _ := decodeErrorPayload(body)
		if msg == "" {
			msg = "server error"
		}
		return &StatusError{Code: status, Message: msg}
	case StatusNotLeader:
		return ErrNotLeader
	default:
		return &StatusError{Code: status, Message: fmt.Sprintf("unknown status %d", status)}
	}
}

// Put stores key -> value on the cluster leader.
func (c *KayaClient) Put(ctx context.Context, key, value []byte) error {
	payload := encodePutPayload(key, value)
	status, body, err := c.roundtripWithRedirect(ctx, OpPut, payload)
	if err != nil {
		return err
	}
	return handleStatus(status, body)
}

// Get reads the value for key. Returns ErrNotFound when the key is absent.
func (c *KayaClient) Get(ctx context.Context, key []byte) ([]byte, error) {
	payload := encodeKeyPayload(key)
	status, body, err := c.roundtripWithRedirect(ctx, OpGet, payload)
	if err != nil {
		return nil, err
	}
	if status == StatusNotFound {
		return nil, ErrNotFound
	}
	if status == StatusOK {
		return decodeValuePayload(body)
	}
	return nil, handleStatus(status, body)
}

// Delete removes key from the cluster leader.
func (c *KayaClient) Delete(ctx context.Context, key []byte) error {
	payload := encodeKeyPayload(key)
	status, body, err := c.roundtripWithRedirect(ctx, OpDelete, payload)
	if err != nil {
		return err
	}
	return handleStatus(status, body)
}

// Scan returns all key/value pairs with the given prefix.
func (c *KayaClient) Scan(ctx context.Context, prefix []byte) ([]KVPair, error) {
	payload := encodeScanPayload(prefix)
	status, body, err := c.roundtripWithRedirect(ctx, OpScan, payload)
	if err != nil {
		return nil, err
	}
	if status == StatusOK {
		return decodeScanResponse(body)
	}
	return nil, handleStatus(status, body)
}

// Health returns the node role string ("leader" or "follower").
func (c *KayaClient) Health(ctx context.Context) (string, error) {
	status, body, err := c.roundtripWithRedirect(ctx, OpHealth, nil)
	if err != nil {
		return "", err
	}
	if status == StatusOK {
		return string(body), nil
	}
	return "", handleStatus(status, body)
}

// Stats returns the server metrics JSON document.
func (c *KayaClient) Stats(ctx context.Context) (string, error) {
	status, body, err := c.roundtripWithRedirect(ctx, OpStats, nil)
	if err != nil {
		return "", err
	}
	if status == StatusOK {
		return string(body), nil
	}
	return "", handleStatus(status, body)
}