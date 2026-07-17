package kaya

import (
	"encoding/binary"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	MaxFrameLen = 64 * 1024 * 1024

	OpPut           uint8 = 1
	OpGet           uint8 = 2
	OpDelete        uint8 = 3
	OpScan          uint8 = 4
	OpHealth        uint8 = 5
	OpStats         uint8 = 6
	OpTxnBegin      uint8 = 9
	OpTxnOp         uint8 = 10
	OpTxnCommit     uint8 = 11
	OpTxnRollback   uint8 = 12
	OpCdcPoll       uint8 = 13
	OpCdcCheckpoint uint8 = 14

	TxnOpGet    uint8 = 1
	TxnOpPut    uint8 = 2
	TxnOpDelete uint8 = 3

	CdcEventPut    uint8 = 1
	CdcEventDelete uint8 = 2

	StatusOK              uint16 = 0
	StatusInvalidArgument uint16 = 1
	StatusNotFound        uint16 = 2
	StatusTxnConflict     uint16 = 3
	StatusServerError     uint16 = 9
	StatusNotLeader       uint16 = 10
	StatusRangeMoved      uint16 = 11
)

// CdcEvent is a single changefeed record from CDC_POLL.
type CdcEvent struct {
	Seq   uint64
	IsPut bool
	Key   []byte
	Value []byte // nil for deletes
}

// CLIENT\x00 prefix for optional client token framing (matches kaya-net).
var clientAuthPrefix = []byte("CLIENT\x00")

// KVPair is a key/value pair returned by Scan.
type KVPair struct {
	Key   []byte
	Value []byte
}

func encodePutPayload(key, value []byte) []byte {
	buf := make([]byte, 8+len(key)+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(value)))
	copy(buf[8:], key)
	copy(buf[8+len(key):], value)
	return buf
}

func decodePutPayload(data []byte) (key, value []byte, err error) {
	if len(data) < 8 {
		return nil, nil, fmt.Errorf("truncated put payload")
	}
	keyLen := binary.LittleEndian.Uint32(data[0:4])
	valueLen := binary.LittleEndian.Uint32(data[4:8])
	if len(data) < 8+int(keyLen)+int(valueLen) {
		return nil, nil, fmt.Errorf("truncated put payload data")
	}
	key = data[8 : 8+keyLen]
	value = data[8+keyLen : 8+keyLen+valueLen]
	return key, value, nil
}

func encodeKeyPayload(key []byte) []byte {
	buf := make([]byte, 4+len(key))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	copy(buf[4:], key)
	return buf
}

func decodeKeyPayload(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("truncated key payload")
	}
	keyLen := binary.LittleEndian.Uint32(data[0:4])
	if len(data) < 4+int(keyLen) {
		return nil, fmt.Errorf("truncated key")
	}
	return data[4 : 4+keyLen], nil
}

func encodeScanPayload(prefix []byte) []byte {
	return encodeKeyPayload(prefix)
}

func decodeScanPayload(data []byte) ([]byte, error) {
	return decodeKeyPayload(data)
}

func encodeValuePayload(value []byte) []byte {
	buf := make([]byte, 4+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(value)))
	copy(buf[4:], value)
	return buf
}

func decodeValuePayload(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("truncated value payload")
	}
	valueLen := binary.LittleEndian.Uint32(data[0:4])
	if len(data) < 4+int(valueLen) {
		return nil, fmt.Errorf("truncated value")
	}
	return data[4 : 4+valueLen], nil
}

func encodeScanResponse(items []KVPair) []byte {
	size := 4
	for _, item := range items {
		size += 8 + len(item.Key) + len(item.Value)
	}
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(items)))
	offset := 4
	for _, item := range items {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(item.Key)))
		offset += 4
		copy(buf[offset:], item.Key)
		offset += len(item.Key)
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(item.Value)))
		offset += 4
		copy(buf[offset:], item.Value)
		offset += len(item.Value)
	}
	return buf
}

func decodeScanResponse(data []byte) ([]KVPair, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("truncated scan response")
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	if count > 1_000_000 {
		return nil, fmt.Errorf("suspiciously large scan count: %d", count)
	}

	out := make([]KVPair, 0, count)
	cur := data[4:]
	for i := uint32(0); i < count; i++ {
		if len(cur) < 4 {
			return nil, fmt.Errorf("truncated key len in scan")
		}
		keyLen := binary.LittleEndian.Uint32(cur[0:4])
		cur = cur[4:]
		if len(cur) < int(keyLen) {
			return nil, fmt.Errorf("truncated key in scan")
		}
		key := cur[:keyLen]
		cur = cur[keyLen:]

		if len(cur) < 4 {
			return nil, fmt.Errorf("truncated value len in scan")
		}
		valueLen := binary.LittleEndian.Uint32(cur[0:4])
		cur = cur[4:]
		if len(cur) < int(valueLen) {
			return nil, fmt.Errorf("truncated value in scan")
		}
		value := cur[:valueLen]
		cur = cur[valueLen:]

		out = append(out, KVPair{Key: key, Value: value})
	}
	return out, nil
}

func encodeErrorPayload(msg string) []byte {
	msgBytes := []byte(msg)
	buf := make([]byte, 4+len(msgBytes))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(msgBytes)))
	copy(buf[4:], msgBytes)
	return buf
}

func decodeErrorPayload(data []byte) (string, error) {
	if len(data) < 4 {
		return "", fmt.Errorf("truncated error payload")
	}
	msgLen := binary.LittleEndian.Uint32(data[0:4])
	if len(data) < 4+int(msgLen) {
		return "", fmt.Errorf("truncated error message")
	}
	msgBytes := data[4 : 4+msgLen]
	if !utf8.Valid(msgBytes) {
		return "", fmt.Errorf("invalid UTF-8 in error payload")
	}
	return string(msgBytes), nil
}

// encodeTxnBeginResponse encodes TXN_BEGIN OK: txn_id(u64) | snapshot_ts(u64).
func encodeTxnBeginResponse(txnID, snapshotTS uint64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], txnID)
	binary.LittleEndian.PutUint64(buf[8:16], snapshotTS)
	return buf
}

// decodeTxnBeginResponse decodes TXN_BEGIN OK body.
func decodeTxnBeginResponse(data []byte) (txnID, snapshotTS uint64, err error) {
	if len(data) < 16 {
		return 0, 0, fmt.Errorf("truncated txn begin response")
	}
	txnID = binary.LittleEndian.Uint64(data[0:8])
	snapshotTS = binary.LittleEndian.Uint64(data[8:16])
	return txnID, snapshotTS, nil
}

// encodeTxnOpPayload encodes TXN_OP:
// txn_id(u64) | op(u8) | key_len(u32) | key | [value_len(u32) | value for put].
func encodeTxnOpPayload(txnID uint64, op uint8, key, value []byte) []byte {
	size := 8 + 1 + 4 + len(key)
	if op == TxnOpPut {
		size += 4 + len(value)
	}
	buf := make([]byte, size)
	binary.LittleEndian.PutUint64(buf[0:8], txnID)
	buf[8] = op
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(key)))
	copy(buf[13:], key)
	if op == TxnOpPut {
		off := 13 + len(key)
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(value)))
		copy(buf[off+4:], value)
	}
	return buf
}

// decodeTxnOpPayload decodes a TXN_OP request body.
func decodeTxnOpPayload(data []byte) (txnID uint64, op uint8, key, value []byte, err error) {
	if len(data) < 8+1+4 {
		return 0, 0, nil, nil, fmt.Errorf("truncated txn op payload")
	}
	txnID = binary.LittleEndian.Uint64(data[0:8])
	op = data[8]
	keyLen := binary.LittleEndian.Uint32(data[9:13])
	cur := data[13:]
	if len(cur) < int(keyLen) {
		return 0, 0, nil, nil, fmt.Errorf("truncated txn op key")
	}
	key = append([]byte(nil), cur[:keyLen]...)
	cur = cur[keyLen:]
	switch op {
	case TxnOpPut:
		if len(cur) < 4 {
			return 0, 0, nil, nil, fmt.Errorf("truncated txn op value len")
		}
		valueLen := binary.LittleEndian.Uint32(cur[0:4])
		cur = cur[4:]
		if len(cur) < int(valueLen) {
			return 0, 0, nil, nil, fmt.Errorf("truncated txn op value")
		}
		value = append([]byte(nil), cur[:valueLen]...)
	case TxnOpGet, TxnOpDelete:
		// no value
	default:
		return 0, 0, nil, nil, fmt.Errorf("unknown TXN_OP kind: %d", op)
	}
	return txnID, op, key, value, nil
}

// encodeTxnIDPayload encodes TXN_COMMIT / TXN_ROLLBACK: txn_id(u64).
func encodeTxnIDPayload(txnID uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, txnID)
	return buf
}

// decodeTxnIDPayload decodes a txn_id body.
func decodeTxnIDPayload(data []byte) (uint64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("truncated txn id payload")
	}
	return binary.LittleEndian.Uint64(data[0:8]), nil
}

// encodeTxnCommitResponse encodes TXN_COMMIT OK: commit_ts(u64).
func encodeTxnCommitResponse(commitTS uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, commitTS)
	return buf
}

// decodeTxnCommitResponse decodes TXN_COMMIT OK body.
func decodeTxnCommitResponse(data []byte) (uint64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("truncated txn commit response")
	}
	return binary.LittleEndian.Uint64(data[0:8]), nil
}

func encodeCdcPollRequest(consumerID string, fromSeq uint64, limit uint32) []byte {
	id := []byte(consumerID)
	buf := make([]byte, 2+len(id)+8+4)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(id)))
	copy(buf[2:], id)
	binary.LittleEndian.PutUint64(buf[2+len(id):2+len(id)+8], fromSeq)
	binary.LittleEndian.PutUint32(buf[2+len(id)+8:], limit)
	return buf
}

func decodeCdcPollResponse(data []byte) ([]CdcEvent, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("truncated cdc poll response")
	}
	count := binary.LittleEndian.Uint32(data[0:4])
	cur := data[4:]
	out := make([]CdcEvent, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(cur) < 8+1+4 {
			return nil, fmt.Errorf("truncated cdc event header")
		}
		seq := binary.LittleEndian.Uint64(cur[0:8])
		op := cur[8]
		cur = cur[9:]
		keyLen := binary.LittleEndian.Uint32(cur[0:4])
		cur = cur[4:]
		if len(cur) < int(keyLen) {
			return nil, fmt.Errorf("truncated cdc event key")
		}
		key := append([]byte(nil), cur[:keyLen]...)
		cur = cur[keyLen:]
		var value []byte
		isPut := op == CdcEventPut
		if isPut {
			if len(cur) < 4 {
				return nil, fmt.Errorf("truncated cdc event value len")
			}
			valueLen := binary.LittleEndian.Uint32(cur[0:4])
			cur = cur[4:]
			if len(cur) < int(valueLen) {
				return nil, fmt.Errorf("truncated cdc event value")
			}
			value = append([]byte(nil), cur[:valueLen]...)
			cur = cur[valueLen:]
		} else if op != CdcEventDelete {
			return nil, fmt.Errorf("unknown cdc event op %d", op)
		}
		out = append(out, CdcEvent{Seq: seq, IsPut: isPut, Key: key, Value: value})
	}
	return out, nil
}

func encodeCdcCheckpointRequest(consumerID string) []byte {
	id := []byte(consumerID)
	buf := make([]byte, 2+len(id))
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(id)))
	copy(buf[2:], id)
	return buf
}

// encodeClientAuthPayload optionally prefixes inner with CLIENT\x00 | token_len(u16) | token.
func encodeClientAuthPayload(inner []byte, clientToken *string) []byte {
	if clientToken == nil || *clientToken == "" {
		out := make([]byte, len(inner))
		copy(out, inner)
		return out
	}
	token := []byte(*clientToken)
	out := make([]byte, len(clientAuthPrefix)+2+len(token)+len(inner))
	offset := 0
	copy(out[offset:], clientAuthPrefix)
	offset += len(clientAuthPrefix)
	binary.LittleEndian.PutUint16(out[offset:offset+2], uint16(len(token)))
	offset += 2
	copy(out[offset:], token)
	offset += len(token)
	copy(out[offset:], inner)
	return out
}

func decodeClientAuthPayload(data []byte) (inner []byte, token *string, err error) {
	cur := data
	if len(cur) >= len(clientAuthPrefix) && string(cur[:len(clientAuthPrefix)]) == string(clientAuthPrefix) {
		cur = cur[len(clientAuthPrefix):]
		if len(cur) < 2 {
			return nil, nil, fmt.Errorf("truncated client auth token length")
		}
		tokenLen := binary.LittleEndian.Uint16(cur[0:2])
		cur = cur[2:]
		if len(cur) < int(tokenLen) {
			return nil, nil, fmt.Errorf("truncated client auth token")
		}
		tokenBytes := cur[:tokenLen]
		if !utf8.Valid(tokenBytes) {
			return nil, nil, fmt.Errorf("invalid utf-8 in client token")
		}
		tok := string(tokenBytes)
		token = &tok
		cur = cur[tokenLen:]
	}
	inner = append([]byte(nil), cur...)
	return inner, token, nil
}

func encodeClientFrame(opcode uint8, payload []byte) []byte {
	frameLen := uint32(1 + len(payload))
	buf := make([]byte, 4+1+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], frameLen)
	buf[4] = opcode
	copy(buf[5:], payload)
	return buf
}

func readResponse(r io.Reader) (status uint16, payload []byte, err error) {
	var lenBuf [4]byte
	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	frameLen := binary.LittleEndian.Uint32(lenBuf[:])
	if frameLen < 2 || frameLen > MaxFrameLen {
		return 0, nil, fmt.Errorf("%w: invalid response frame length: %d", ErrProtocol, frameLen)
	}

	body := make([]byte, frameLen)
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}

	status = binary.LittleEndian.Uint16(body[0:2])
	payload = body[2:]
	return status, payload, nil
}