package kaya

import (
	"encoding/binary"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	MaxFrameLen = 64 * 1024 * 1024

	OpPut    uint8 = 1
	OpGet    uint8 = 2
	OpDelete uint8 = 3
	OpScan   uint8 = 4
	OpHealth uint8 = 5
	OpStats  uint8 = 6

	StatusOK               uint16 = 0
	StatusInvalidArgument  uint16 = 1
	StatusNotFound         uint16 = 2
	StatusServerError      uint16 = 9
	StatusNotLeader        uint16 = 10
)

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