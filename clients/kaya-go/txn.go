package kaya

import (
	"context"
	"fmt"
)

// localWrite holds a staged put (value set) or delete (deleted=true).
type localWrite struct {
	value   []byte
	deleted bool
}

// Transaction is a Snapshot Isolation handle obtained via KayaClient.BeginTxn.
//
// Writes are staged as intents on the leader; Commit materializes them and
// Rollback discards them. A local write buffer provides client-side
// read-your-writes for keys written in this transaction.
//
// Cross-range (multi-group) commits are handled by the server via 2PC; the
// client wire path is unchanged (TXN_BEGIN / TXN_OP / TXN_COMMIT).
type Transaction struct {
	client     *KayaClient
	txnID      uint64
	snapshotTS uint64
	local      map[string]localWrite
	done       bool
}

// TxnID returns the server-assigned transaction id.
func (t *Transaction) TxnID() uint64 { return t.txnID }

// SnapshotTS returns the snapshot / read timestamp for this transaction.
func (t *Transaction) SnapshotTS() uint64 { return t.snapshotTS }

// BeginTxn starts a Snapshot Isolation transaction on the leader.
func (c *KayaClient) BeginTxn(ctx context.Context) (*Transaction, error) {
	status, body, err := c.roundtripWithRedirect(ctx, OpTxnBegin, nil)
	if err != nil {
		return nil, err
	}
	if status == StatusOK {
		txnID, snapshotTS, err := decodeTxnBeginResponse(body)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrProtocol, err)
		}
		return &Transaction{
			client:     c,
			txnID:      txnID,
			snapshotTS: snapshotTS,
			local:      make(map[string]localWrite),
		}, nil
	}
	return nil, mapTxnStatus(status, body)
}

// Get reads key under the transaction snapshot, with local read-your-writes.
// Returns ErrNotFound when the key is absent (or deleted in this txn).
func (t *Transaction) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := t.ensureOpen(); err != nil {
		return nil, err
	}
	if w, ok := t.local[string(key)]; ok {
		if w.deleted {
			return nil, ErrNotFound
		}
		out := make([]byte, len(w.value))
		copy(out, w.value)
		return out, nil
	}
	payload := encodeTxnOpPayload(t.txnID, TxnOpGet, key, nil)
	status, body, err := t.client.roundtripWithRedirect(ctx, OpTxnOp, payload)
	if err != nil {
		return nil, err
	}
	if status == StatusOK {
		return decodeValuePayload(body)
	}
	if status == StatusNotFound {
		return nil, ErrNotFound
	}
	return nil, mapTxnStatus(status, body)
}

// Put stages a put intent (write-write conflicts may fail immediately).
func (t *Transaction) Put(ctx context.Context, key, value []byte) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}
	payload := encodeTxnOpPayload(t.txnID, TxnOpPut, key, value)
	status, body, err := t.client.roundtripWithRedirect(ctx, OpTxnOp, payload)
	if err != nil {
		return err
	}
	if status == StatusOK {
		v := make([]byte, len(value))
		copy(v, value)
		t.local[string(key)] = localWrite{value: v}
		return nil
	}
	return mapTxnStatus(status, body)
}

// Delete stages a delete intent.
func (t *Transaction) Delete(ctx context.Context, key []byte) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}
	payload := encodeTxnOpPayload(t.txnID, TxnOpDelete, key, nil)
	status, body, err := t.client.roundtripWithRedirect(ctx, OpTxnOp, payload)
	if err != nil {
		return err
	}
	if status == StatusOK {
		t.local[string(key)] = localWrite{deleted: true}
		return nil
	}
	return mapTxnStatus(status, body)
}

// Commit materializes staged intents. Returns the commit timestamp on success.
// The transaction is marked done even on failure so it cannot be reused.
func (t *Transaction) Commit(ctx context.Context) (commitTS uint64, err error) {
	if err := t.ensureOpen(); err != nil {
		return 0, err
	}
	t.done = true
	payload := encodeTxnIDPayload(t.txnID)
	status, body, err := t.client.roundtripWithRedirect(ctx, OpTxnCommit, payload)
	if err != nil {
		return 0, err
	}
	if status == StatusOK {
		ts, err := decodeTxnCommitResponse(body)
		if err != nil {
			return 0, fmt.Errorf("%w: %v", ErrProtocol, err)
		}
		return ts, nil
	}
	return 0, mapTxnStatus(status, body)
}

// Rollback discards staged intents without committing.
func (t *Transaction) Rollback(ctx context.Context) error {
	if err := t.ensureOpen(); err != nil {
		return err
	}
	t.done = true
	payload := encodeTxnIDPayload(t.txnID)
	status, body, err := t.client.roundtripWithRedirect(ctx, OpTxnRollback, payload)
	if err != nil {
		return err
	}
	if status == StatusOK {
		return nil
	}
	return mapTxnStatus(status, body)
}

func (t *Transaction) ensureOpen() error {
	if t == nil || t.client == nil {
		return fmt.Errorf("%w: nil transaction", ErrInvalidArgument)
	}
	if t.done {
		return fmt.Errorf("%w: transaction already finished", ErrInvalidArgument)
	}
	return nil
}

func mapTxnStatus(status uint16, body []byte) error {
	switch status {
	case StatusTxnConflict:
		return ErrTxnConflict
	case StatusNotFound:
		return ErrNotFound
	case StatusInvalidArgument:
		msg, _ := decodeErrorPayload(body)
		if msg == "" {
			msg = "invalid argument"
		}
		return &StatusError{Code: status, Message: msg}
	case StatusNotLeader:
		return ErrNotLeader
	case StatusServerError:
		msg, _ := decodeErrorPayload(body)
		if msg == "" {
			msg = "server error"
		}
		return &StatusError{Code: status, Message: msg}
	default:
		msg, _ := decodeErrorPayload(body)
		if msg == "" {
			msg = fmt.Sprintf("unknown status %d", status)
		}
		return &StatusError{Code: status, Message: msg}
	}
}
