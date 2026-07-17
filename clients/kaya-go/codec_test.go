package kaya

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestEncodeDecodePutPayload(t *testing.T) {
	cases := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("mykey"), []byte("myvalue")},
		{[]byte("a"), []byte("b")},
		{[]byte(""), []byte("value")},
		{[]byte("key"), []byte("")},
		{[]byte(""), []byte("")},
		{[]byte("キー"), []byte("値")},
		{[]byte{0x00, 0xff, 0x01}, []byte{0xde, 0xad, 0xbe, 0xef}},
	}

	for _, tc := range cases {
		encoded := encodePutPayload(tc.key, tc.value)
		key, value, err := decodePutPayload(encoded)
		if err != nil {
			t.Fatalf("decode put (%q,%q): %v", tc.key, tc.value, err)
		}
		if !bytes.Equal(key, tc.key) || !bytes.Equal(value, tc.value) {
			t.Fatalf("put roundtrip mismatch: got (%q,%q)", key, value)
		}
	}
}

func TestDecodePutPayloadErrors(t *testing.T) {
	if _, _, err := decodePutPayload(nil); err == nil {
		t.Fatal("expected error for empty put payload")
	}
	if _, _, err := decodePutPayload(mustHex("0500")); err == nil {
		t.Fatal("expected error for truncated put header")
	}
}

func TestEncodeDecodeKeyPayload(t *testing.T) {
	cases := [][]byte{
		[]byte("mykey"),
		[]byte(""),
		[]byte("プレフィックス"),
		mustHex("cafebabe"),
		mustHex("7a"),
	}

	for _, key := range cases {
		encoded := encodeKeyPayload(key)
		decoded, err := decodeKeyPayload(encoded)
		if err != nil {
			t.Fatalf("decode key %q: %v", key, err)
		}
		if !bytes.Equal(decoded, key) {
			t.Fatalf("key roundtrip mismatch: got %q", decoded)
		}
	}
}

func TestDecodeKeyPayloadErrors(t *testing.T) {
	if _, err := decodeKeyPayload(nil); err == nil {
		t.Fatal("expected error for empty key payload")
	}
	if _, err := decodeKeyPayload(mustHex("64000000")); err == nil {
		t.Fatal("expected error for truncated key data")
	}
}

func TestEncodeDecodeScanPayload(t *testing.T) {
	prefix := []byte("user:")
	encoded := encodeScanPayload(prefix)
	decoded, err := decodeScanPayload(encoded)
	if err != nil {
		t.Fatalf("decode scan payload: %v", err)
	}
	if !bytes.Equal(decoded, prefix) {
		t.Fatalf("scan payload mismatch: got %q", decoded)
	}
}

func TestEncodeDecodeValuePayload(t *testing.T) {
	value := []byte("world")
	encoded := encodeValuePayload(value)
	decoded, err := decodeValuePayload(encoded)
	if err != nil {
		t.Fatalf("decode value: %v", err)
	}
	if !bytes.Equal(decoded, value) {
		t.Fatalf("value mismatch: got %q", decoded)
	}
}

func TestDecodeValuePayloadErrors(t *testing.T) {
	if _, err := decodeValuePayload(nil); err == nil {
		t.Fatal("expected error for empty value payload")
	}
}

func TestEncodeDecodeScanResponse(t *testing.T) {
	cases := [][]KVPair{
		{},
		{{Key: []byte("a"), Value: []byte("1")}},
		{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Value: []byte("2")},
			{Key: []byte("c"), Value: []byte("3")},
		},
		{
			{Key: []byte("キー"), Value: []byte("値")},
			{Key: []byte("🔑"), Value: []byte("📦")},
		},
		{
			{Key: mustHex("00ff"), Value: mustHex("baad")},
			{Key: mustHex("01"), Value: mustHex("02")},
		},
	}

	for _, items := range cases {
		encoded := encodeScanResponse(items)
		decoded, err := decodeScanResponse(encoded)
		if err != nil {
			t.Fatalf("decode scan response: %v", err)
		}
		if len(decoded) != len(items) {
			t.Fatalf("scan count mismatch: got %d want %d", len(decoded), len(items))
		}
		for i := range items {
			if !bytes.Equal(decoded[i].Key, items[i].Key) || !bytes.Equal(decoded[i].Value, items[i].Value) {
				t.Fatalf("scan item %d mismatch", i)
			}
		}
	}
}

func TestDecodeScanResponseErrors(t *testing.T) {
	if _, err := decodeScanResponse(nil); err == nil {
		t.Fatal("expected error for empty scan response")
	}
	if _, err := decodeScanResponse(mustHex("00842e45")); err == nil {
		t.Fatal("expected error for oversized scan count")
	}
}

func TestEncodeDecodeErrorPayload(t *testing.T) {
	cases := []string{
		"not found",
		"",
		"見つかりません",
		"invalid argument: key too long",
	}

	for _, msg := range cases {
		encoded := encodeErrorPayload(msg)
		decoded, err := decodeErrorPayload(encoded)
		if err != nil {
			t.Fatalf("decode error %q: %v", msg, err)
		}
		if decoded != msg {
			t.Fatalf("error roundtrip mismatch: got %q", decoded)
		}
	}
}

func TestDecodeErrorPayloadInvalidUTF8(t *testing.T) {
	if _, err := decodeErrorPayload(mustHex("03000000fffefd")); err == nil {
		t.Fatal("expected error for invalid utf-8 error payload")
	}
}

func TestEncodeDecodeClientAuthPayload(t *testing.T) {
	token := "client-secret"
	inner := []byte("put-payload-bytes")
	encoded := encodeClientAuthPayload(inner, &token)
	decodedInner, decodedToken, err := decodeClientAuthPayload(encoded)
	if err != nil {
		t.Fatalf("decode client auth: %v", err)
	}
	if !bytes.Equal(decodedInner, inner) {
		t.Fatalf("inner mismatch: got %q", decodedInner)
	}
	if decodedToken == nil || *decodedToken != token {
		t.Fatalf("token mismatch: got %v", decodedToken)
	}

	noTokenInner := mustHex("030000000061")
	encodedNoToken := encodeClientAuthPayload(noTokenInner, nil)
	decodedNoTokenInner, decodedNoToken, err := decodeClientAuthPayload(encodedNoToken)
	if err != nil {
		t.Fatalf("decode client auth without token: %v", err)
	}
	if !bytes.Equal(decodedNoTokenInner, noTokenInner) {
		t.Fatalf("no-token inner mismatch")
	}
	if decodedNoToken != nil {
		t.Fatal("expected nil token")
	}

	unicodeToken := "クライアント"
	encodedUnicode := encodeClientAuthPayload(mustHex("010203"), &unicodeToken)
	_, gotToken, err := decodeClientAuthPayload(encodedUnicode)
	if err != nil {
		t.Fatalf("decode unicode token: %v", err)
	}
	if gotToken == nil || *gotToken != unicodeToken {
		t.Fatalf("unicode token mismatch: got %v", gotToken)
	}
}

func TestEncodeDecodeTxnBeginResponse(t *testing.T) {
	cases := []struct {
		txnID      uint64
		snapshotTS uint64
	}{
		{0, 0},
		{1, 0},
		{7, 42},
		{^uint64(0), 99},
	}
	for _, tc := range cases {
		encoded := encodeTxnBeginResponse(tc.txnID, tc.snapshotTS)
		id, ts, err := decodeTxnBeginResponse(encoded)
		if err != nil {
			t.Fatalf("decode txn begin: %v", err)
		}
		if id != tc.txnID || ts != tc.snapshotTS {
			t.Fatalf("txn begin mismatch: got (%d,%d) want (%d,%d)", id, ts, tc.txnID, tc.snapshotTS)
		}
	}
	if _, _, err := decodeTxnBeginResponse(nil); err == nil {
		t.Fatal("expected error for empty txn begin")
	}
}

func TestEncodeDecodeTxnOpPayload(t *testing.T) {
	// GET
	get := encodeTxnOpPayload(7, TxnOpGet, []byte("k"), nil)
	id, op, key, val, err := decodeTxnOpPayload(get)
	if err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if id != 7 || op != TxnOpGet || string(key) != "k" || val != nil {
		t.Fatalf("get mismatch: id=%d op=%d key=%q val=%v", id, op, key, val)
	}

	// PUT
	put := encodeTxnOpPayload(2, TxnOpPut, []byte("k"), []byte("v"))
	id, op, key, val, err = decodeTxnOpPayload(put)
	if err != nil {
		t.Fatalf("decode put: %v", err)
	}
	if id != 2 || op != TxnOpPut || string(key) != "k" || string(val) != "v" {
		t.Fatalf("put mismatch: id=%d op=%d key=%q val=%q", id, op, key, val)
	}

	// DELETE
	del := encodeTxnOpPayload(3, TxnOpDelete, []byte("k"), nil)
	id, op, key, val, err = decodeTxnOpPayload(del)
	if err != nil {
		t.Fatalf("decode delete: %v", err)
	}
	if id != 3 || op != TxnOpDelete || string(key) != "k" || val != nil {
		t.Fatalf("delete mismatch: id=%d op=%d key=%q val=%v", id, op, key, val)
	}

	// empty key put
	empty := encodeTxnOpPayload(1, TxnOpPut, []byte{}, []byte{})
	id, op, key, val, err = decodeTxnOpPayload(empty)
	if err != nil {
		t.Fatalf("decode empty put: %v", err)
	}
	if id != 1 || op != TxnOpPut || len(key) != 0 || len(val) != 0 {
		t.Fatalf("empty put mismatch")
	}

	if _, _, _, _, err := decodeTxnOpPayload(mustHex("01000000")); err == nil {
		t.Fatal("expected error for truncated txn op")
	}
	// unknown op kind (op=9)
	bad := encodeTxnOpPayload(1, 9, []byte("k"), nil)
	if _, _, _, _, err := decodeTxnOpPayload(bad); err == nil {
		t.Fatal("expected error for unknown txn op kind")
	}
}

func TestEncodeDecodeTxnIDAndCommit(t *testing.T) {
	for _, id := range []uint64{0, 1, 7, 42, ^uint64(0)} {
		enc := encodeTxnIDPayload(id)
		got, err := decodeTxnIDPayload(enc)
		if err != nil {
			t.Fatalf("decode txn id: %v", err)
		}
		if got != id {
			t.Fatalf("txn id mismatch: got %d want %d", got, id)
		}
	}
	if _, err := decodeTxnIDPayload(nil); err == nil {
		t.Fatal("expected error for empty txn id")
	}

	for _, ts := range []uint64{0, 12, 99, ^uint64(0)} {
		enc := encodeTxnCommitResponse(ts)
		got, err := decodeTxnCommitResponse(enc)
		if err != nil {
			t.Fatalf("decode commit ts: %v", err)
		}
		if got != ts {
			t.Fatalf("commit ts mismatch: got %d want %d", got, ts)
		}
	}
	if _, err := decodeTxnCommitResponse(nil); err == nil {
		t.Fatal("expected error for empty commit response")
	}
}

func TestTxnOpcodesMatchWire(t *testing.T) {
	if OpTxnBegin != 9 || OpTxnOp != 10 || OpTxnCommit != 11 || OpTxnRollback != 12 {
		t.Fatalf("txn opcodes mismatch: begin=%d op=%d commit=%d rollback=%d",
			OpTxnBegin, OpTxnOp, OpTxnCommit, OpTxnRollback)
	}
	if TxnOpGet != 1 || TxnOpPut != 2 || TxnOpDelete != 3 {
		t.Fatalf("txn op kinds mismatch")
	}
	if StatusTxnConflict != 3 {
		t.Fatalf("STATUS_TXN_CONFLICT = %d, want 3", StatusTxnConflict)
	}
}

func TestEncodeClientFramePutHelloWorld(t *testing.T) {
	payload := encodePutPayload([]byte("hello"), []byte("world"))
	frame := encodeClientFrame(OpPut, payload)

	// frame_len = 1 (opcode) + 16 (payload) = 17... payload is 18 bytes (4+4+5+5),
	// so frame_len = 19.
	want, err := hex.DecodeString(
		"13000000" + // frame_len = 19
			"01" + // opcode PUT
			"05000000" + // key_len
			"05000000" + // value_len
			"68656c6c6f" + // hello
			"776f726c64", // world
	)
	if err != nil {
		t.Fatalf("decode want hex: %v", err)
	}
	if !bytes.Equal(frame, want) {
		t.Fatalf("put frame mismatch:\n got %x\nwant %x", frame, want)
	}
}

func TestReadResponseOKGet(t *testing.T) {
	// GET "hello" response OK: frame_len = 2 (status) + 9 (value_len + value) = 11
	raw, err := hex.DecodeString(
		"0b000000" + // frame_len = 11
			"0000" + // status OK
			"05000000" + // value_len
			"776f726c64", // world
	)
	if err != nil {
		t.Fatalf("decode response hex: %v", err)
	}

	status, payload, err := readResponse(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if status != StatusOK {
		t.Fatalf("status = %d, want %d", status, StatusOK)
	}
	value, err := decodeValuePayload(payload)
	if err != nil {
		t.Fatalf("decode value payload: %v", err)
	}
	if string(value) != "world" {
		t.Fatalf("value = %q, want world", value)
	}
}

func TestReadResponseNotLeader(t *testing.T) {
	raw, err := hex.DecodeString(
		"10000000" + // frame_len = 16
			"0a00" + // status NOT_LEADER (10)
			"3132372e302e302e313a37333739", // 127.0.0.1:7379
	)
	if err != nil {
		t.Fatalf("decode response hex: %v", err)
	}

	status, payload, err := readResponse(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if status != StatusNotLeader {
		t.Fatalf("status = %d, want %d", status, StatusNotLeader)
	}
	if string(payload) != "127.0.0.1:7379" {
		t.Fatalf("leader hint = %q", payload)
	}
}

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}