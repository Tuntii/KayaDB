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
	addr         string
	maxRedirects int
	timeout      time.Duration
	conn         net.Conn
	clientToken  *string
	retry        RetryPolicy
	backoffSeed  uint64
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
		retry:        DefaultRetryPolicy(),
		backoffSeed:  seedFromAddr(addr),
	}, nil
}

// SetClientToken sets the token sent with data-path operations
// (PUT/GET/DELETE/SCAN/STATS/TXN_*/CDC).
func (c *KayaClient) SetClientToken(token string) {
	if token == "" {
		c.clientToken = nil
		return
	}
	c.clientToken = &token
}

// SetTimeout configures the fallback per-operation read/write deadline when
// RetryPolicy.RequestTimeout is zero.
func (c *KayaClient) SetTimeout(d time.Duration) {
	c.timeout = d
}

// SetMaxRedirects configures how many NOT_LEADER redirects to follow.
func (c *KayaClient) SetMaxRedirects(n int) {
	c.maxRedirects = n
}

// SetRetryPolicy replaces the transport retry policy (attempts, backoff, jitter,
// per-attempt timeout). Leader redirects keep their own budget via SetMaxRedirects.
func (c *KayaClient) SetRetryPolicy(p RetryPolicy) {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	c.retry = p
}

// RetryPolicy returns a copy of the current retry policy.
func (c *KayaClient) RetryPolicy() RetryPolicy {
	return c.retry
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
	case OpPut, OpGet, OpDelete, OpScan, OpStats,
		OpTxnBegin, OpTxnOp, OpTxnCommit, OpTxnRollback,
		OpCdcPoll, OpCdcCheckpoint:
		return encodeClientAuthPayload(payload, c.clientToken)
	default:
		return payload
	}
}

func (c *KayaClient) attemptTimeout() time.Duration {
	if c.retry.RequestTimeout > 0 {
		return c.retry.RequestTimeout
	}
	return c.timeout
}

func (c *KayaClient) dial(ctx context.Context) (net.Conn, error) {
	timeout := c.attemptTimeout()
	d := net.Dialer{}
	if timeout > 0 {
		d.Timeout = timeout
	}
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

// oneRoundtrip performs a single request/response on the current connection.
func (c *KayaClient) oneRoundtrip(ctx context.Context, opcode uint8, wirePayload []byte) (uint16, []byte, error) {
	if err := c.ensureConn(ctx); err != nil {
		return 0, nil, err
	}

	timeout := c.attemptTimeout()
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
		_ = c.conn.SetWriteDeadline(deadline)
	} else {
		_ = c.conn.SetWriteDeadline(time.Time{})
	}
	// Also respect parent context deadline when tighter.
	if dl, ok := ctx.Deadline(); ok && (deadline.IsZero() || dl.Before(deadline)) {
		_ = c.conn.SetWriteDeadline(dl)
		deadline = dl
	}

	frame := encodeClientFrame(opcode, wirePayload)
	if _, err := c.conn.Write(frame); err != nil {
		c.closeConn()
		return 0, nil, fmt.Errorf("%w: %v", ErrConnection, err)
	}

	if !deadline.IsZero() {
		_ = c.conn.SetReadDeadline(deadline)
	} else {
		_ = c.conn.SetReadDeadline(time.Time{})
	}
	status, body, err := readResponse(c.conn)
	if err != nil {
		c.closeConn()
		// Classify deadline as timeout when possible.
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return 0, nil, ErrTimeout
		}
		return 0, nil, fmt.Errorf("%w: %v", ErrConnection, err)
	}
	return status, body, nil
}

func (c *KayaClient) roundtripWithRedirect(ctx context.Context, opcode uint8, payload []byte) (uint16, []byte, error) {
	wirePayload := c.wirePayload(opcode, payload)
	maxAttempts := c.retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	transportAttempts := 0
	redirects := 0

	for {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}

		status, body, err := c.oneRoundtrip(ctx, opcode, wirePayload)
		if err != nil {
			c.closeConn()
			transportAttempts++
			lastErr = err
			if transportAttempts >= maxAttempts {
				return 0, nil, lastErr
			}
			backoff := c.retry.Backoff(uint32(transportAttempts-1), &c.backoffSeed)
			if backoff > 0 {
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return 0, nil, ctx.Err()
				case <-timer.C:
				}
			}
			continue
		}

		if status == StatusNotLeader {
			c.closeConn()
			if redirects >= c.maxRedirects {
				return status, body, nil
			}
			hint := string(body)
			if hint != "" {
				c.addr = hint
			}
			backoff := c.retry.Backoff(uint32(redirects), &c.backoffSeed)
			redirects++
			if backoff > 0 {
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return 0, nil, ctx.Err()
				case <-timer.C:
				}
			}
			continue
		}

		return status, body, nil
	}
}

func handleStatus(status uint16, body []byte) error {
	switch status {
	case StatusOK:
		return nil
	case StatusNotFound:
		return ErrNotFound
	case StatusTxnConflict:
		return ErrTxnConflict
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

// CdcPoll returns changefeed events with seq > fromSeq, up to limit (at-least-once).
func (c *KayaClient) CdcPoll(ctx context.Context, consumerID string, fromSeq uint64, limit uint32) ([]CdcEvent, error) {
	payload := encodeCdcPollRequest(consumerID, fromSeq, limit)
	status, body, err := c.roundtripWithRedirect(ctx, OpCdcPoll, payload)
	if err != nil {
		return nil, err
	}
	if status == StatusOK {
		return decodeCdcPollResponse(body)
	}
	return nil, handleStatus(status, body)
}

// CdcCheckpoint persists the consumer's last polled sequence on the leader.
func (c *KayaClient) CdcCheckpoint(ctx context.Context, consumerID string) error {
	payload := encodeCdcCheckpointRequest(consumerID)
	status, body, err := c.roundtripWithRedirect(ctx, OpCdcCheckpoint, payload)
	if err != nil {
		return err
	}
	return handleStatus(status, body)
}
