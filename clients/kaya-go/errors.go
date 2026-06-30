package kaya

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound        = errors.New("kaya: key not found")
	ErrInvalidArgument = errors.New("kaya: invalid argument")
	ErrNotLeader       = errors.New("kaya: not leader")
	ErrTimeout         = errors.New("kaya: timeout")
	ErrConnection      = errors.New("kaya: connection error")
	ErrProtocol        = errors.New("kaya: protocol error")
)

// StatusError represents a server response with a non-success status code.
type StatusError struct {
	Code    uint16
	Message string
}

func (e *StatusError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("kaya: server status %d", e.Code)
}