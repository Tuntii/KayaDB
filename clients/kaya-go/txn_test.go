package kaya

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// mockTxnServer speaks a minimal subset of the wire protocol for unit tests.
type mockTxnServer struct {
	ln net.Listener

	mu       sync.Mutex
	nextID   uint64
	txns     map[uint64]*mockTxn
	handlers map[uint8]func(payload []byte) (uint16, []byte)
}

type mockTxn struct {
	snapshotTS uint64
	writes     map[string][]byte // nil value means deleted
	committed  bool
	aborted    bool
}

func startMockTxnServer(t *testing.T) *mockTxnServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &mockTxnServer{
		ln:     ln,
		nextID: 1,
		txns:   make(map[uint64]*mockTxn),
	}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *mockTxnServer) addr() string { return s.ln.Addr().String() }

func (s *mockTxnServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *mockTxnServer) handle(conn net.Conn) {
	defer conn.Close()
	for {
		var lenBuf [4]byte
		if _, err := readFull(conn, lenBuf[:]); err != nil {
			return
		}
		frameLen := uint32(lenBuf[0]) | uint32(lenBuf[1])<<8 | uint32(lenBuf[2])<<16 | uint32(lenBuf[3])<<24
		if frameLen < 1 || frameLen > MaxFrameLen {
			return
		}
		body := make([]byte, frameLen)
		if _, err := readFull(conn, body); err != nil {
			return
		}
		opcode := body[0]
		payload := body[1:]
		// Strip optional CLIENT auth prefix for tests.
		if len(payload) >= len(clientAuthPrefix) && string(payload[:len(clientAuthPrefix)]) == string(clientAuthPrefix) {
			cur := payload[len(clientAuthPrefix):]
			if len(cur) >= 2 {
				tokLen := int(cur[0]) | int(cur[1])<<8
				cur = cur[2:]
				if len(cur) >= tokLen {
					payload = cur[tokLen:]
				}
			}
		}

		status, resp := s.dispatch(opcode, payload)
		frame := encodeServerFrame(status, resp)
		if _, err := conn.Write(frame); err != nil {
			return
		}
	}
}

func (s *mockTxnServer) dispatch(opcode uint8, payload []byte) (uint16, []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch opcode {
	case OpTxnBegin:
		id := s.nextID
		s.nextID++
		s.txns[id] = &mockTxn{
			snapshotTS: id * 10,
			writes:     make(map[string][]byte),
		}
		return StatusOK, encodeTxnBeginResponse(id, id*10)
	case OpTxnOp:
		txnID, op, key, value, err := decodeTxnOpPayload(payload)
		if err != nil {
			return StatusInvalidArgument, encodeErrorPayload(err.Error())
		}
		txn, ok := s.txns[txnID]
		if !ok || txn.committed || txn.aborted {
			return StatusInvalidArgument, encodeErrorPayload("unknown txn")
		}
		switch op {
		case TxnOpGet:
			if v, ok := txn.writes[string(key)]; ok {
				if v == nil {
					return StatusNotFound, nil
				}
				return StatusOK, encodeValuePayload(v)
			}
			// No durable store in mock — treat missing as not found.
			return StatusNotFound, nil
		case TxnOpPut:
			v := append([]byte(nil), value...)
			txn.writes[string(key)] = v
			return StatusOK, nil
		case TxnOpDelete:
			txn.writes[string(key)] = nil
			return StatusOK, nil
		default:
			return StatusInvalidArgument, encodeErrorPayload("bad op")
		}
	case OpTxnCommit:
		txnID, err := decodeTxnIDPayload(payload)
		if err != nil {
			return StatusInvalidArgument, encodeErrorPayload(err.Error())
		}
		txn, ok := s.txns[txnID]
		if !ok || txn.committed || txn.aborted {
			return StatusInvalidArgument, encodeErrorPayload("unknown txn")
		}
		// Simulate conflict when key "conflict" was written.
		if _, hit := txn.writes["conflict"]; hit {
			return StatusTxnConflict, encodeErrorPayload("txn conflict")
		}
		txn.committed = true
		return StatusOK, encodeTxnCommitResponse(txn.snapshotTS + 1)
	case OpTxnRollback:
		txnID, err := decodeTxnIDPayload(payload)
		if err != nil {
			return StatusInvalidArgument, encodeErrorPayload(err.Error())
		}
		txn, ok := s.txns[txnID]
		if !ok {
			return StatusInvalidArgument, encodeErrorPayload("unknown txn")
		}
		txn.aborted = true
		return StatusOK, nil
	case OpHealth:
		return StatusOK, []byte("leader")
	default:
		return StatusServerError, encodeErrorPayload("unsupported")
	}
}

func encodeServerFrame(status uint16, payload []byte) []byte {
	frameLen := uint32(2 + len(payload))
	buf := make([]byte, 4+2+len(payload))
	buf[0] = byte(frameLen)
	buf[1] = byte(frameLen >> 8)
	buf[2] = byte(frameLen >> 16)
	buf[3] = byte(frameLen >> 24)
	buf[4] = byte(status)
	buf[5] = byte(status >> 8)
	copy(buf[6:], payload)
	return buf
}

func readFull(r net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func TestTxnBeginPutGetCommit(t *testing.T) {
	srv := startMockTxnServer(t)
	client, err := Connect(srv.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetRetryPolicy(RetryPolicyNone())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	txn, err := client.BeginTxn(ctx)
	if err != nil {
		t.Fatalf("BeginTxn: %v", err)
	}
	if txn.TxnID() == 0 {
		t.Fatal("expected non-zero txn id")
	}
	if txn.SnapshotTS() != txn.TxnID()*10 {
		t.Fatalf("snapshot ts = %d", txn.SnapshotTS())
	}

	if err := txn.Put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Local read-your-writes.
	val, err := txn.Get(ctx, []byte("a"))
	if err != nil {
		t.Fatalf("Get local: %v", err)
	}
	if string(val) != "1" {
		t.Fatalf("Get = %q, want 1", val)
	}

	// Server-side get after put (same key, already local).
	if err := txn.Put(ctx, []byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put b: %v", err)
	}

	ts, err := txn.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if ts == 0 {
		t.Fatal("expected non-zero commit ts")
	}

	// Finished txn cannot be reused.
	if err := txn.Put(ctx, []byte("c"), []byte("3")); err == nil {
		t.Fatal("expected error after commit")
	}
}

func TestTxnRollback(t *testing.T) {
	srv := startMockTxnServer(t)
	client, err := Connect(srv.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetRetryPolicy(RetryPolicyNone())

	ctx := context.Background()
	txn, err := client.BeginTxn(ctx)
	if err != nil {
		t.Fatalf("BeginTxn: %v", err)
	}
	if err := txn.Put(ctx, []byte("x"), []byte("y")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := txn.Delete(ctx, []byte("x")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Local delete => not found.
	if _, err := txn.Get(ctx, []byte("x")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete: %v", err)
	}
	if err := txn.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

func TestTxnConflict(t *testing.T) {
	srv := startMockTxnServer(t)
	client, err := Connect(srv.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetRetryPolicy(RetryPolicyNone())

	ctx := context.Background()
	txn, err := client.BeginTxn(ctx)
	if err != nil {
		t.Fatalf("BeginTxn: %v", err)
	}
	if err := txn.Put(ctx, []byte("conflict"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, err = txn.Commit(ctx)
	if !errors.Is(err, ErrTxnConflict) {
		t.Fatalf("Commit error = %v, want ErrTxnConflict", err)
	}
}

func TestTxnGetServerNotFound(t *testing.T) {
	srv := startMockTxnServer(t)
	client, err := Connect(srv.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetRetryPolicy(RetryPolicyNone())

	ctx := context.Background()
	txn, err := client.BeginTxn(ctx)
	if err != nil {
		t.Fatalf("BeginTxn: %v", err)
	}
	_, err = txn.Get(ctx, []byte("missing"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
	if err := txn.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}
