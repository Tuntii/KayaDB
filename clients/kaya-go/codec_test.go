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