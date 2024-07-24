package ydb

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestIsTransportError(t *testing.T) {
	code := TransportErrorCanceled
	for _, err := range []error{
		&TransportError{Reason: code},
		&TransportError{Reason: code, err: context.Canceled},
		fmt.Errorf("wrapped: %w", &TransportError{Reason: code}),
	} {
		t.Run("", func(t *testing.T) {
			if !IsTransportError(err, code) {
				t.Errorf("expected %v to be TransportError with code=%v", err, code)
			}
		})
	}
}

func TestIsNotTransportError(t *testing.T) {
	code := TransportErrorCanceled
	for _, err := range []error{
		&TransportError{Reason: TransportErrorAborted},
		&TransportError{Reason: TransportErrorAborted, err: context.Canceled},
		fmt.Errorf("wrapped: %w", &TransportError{Reason: TransportErrorAborted}),
		&OpError{Reason: StatusBadRequest},
	} {
		t.Run("", func(t *testing.T) {
			if IsTransportError(err, code) {
				t.Errorf("expected %v not to be TransportError with code=%v", err, code)
			}
		})
	}
}

func TestTransportErrorWrapsContextError(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &TransportError{
		Reason: TransportErrorCanceled,
		err:    context.Canceled,
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected %v to wrap context.Canceled", err)
	}
}

func TestIsOpError(t *testing.T) {
	code := StatusBadRequest
	for _, err := range []error{
		&OpError{Reason: code},
		fmt.Errorf("wrapped: %w", &OpError{Reason: code}),
	} {
		t.Run("", func(t *testing.T) {
			if !IsOpError(err, code) {
				t.Errorf("expected %v to be OpError with code=%v", err, code)
			}
		})
	}
}

func TestIsNotOpError(t *testing.T) {
	code := StatusBadRequest
	for _, err := range []error{
		&OpError{Reason: StatusTimeout},
		fmt.Errorf("wrapped: %w", &OpError{Reason: StatusTimeout}),
		&TransportError{Reason: TransportErrorAborted},
	} {
		t.Run("", func(t *testing.T) {
			if IsOpError(err, code) {
				t.Errorf("expected %v not to be OpError with code=%v", err, code)
			}
		})
	}
}
