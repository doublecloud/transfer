package ydb

import (
	"errors"
	"time"
)

var (
	// DefaultDiscoveryInterval contains default duration between discovery
	// requests made by driver.
	DefaultDiscoveryInterval = time.Minute

	// DefaultBalancingMethod contains driver's default balancing algorithm.
	DefaultBalancingMethod = BalancingRandomChoice

	// ErrClosed is returned when operation requested on a closed driver.
	ErrClosed = errors.New("driver closed")

	// ErrNilConnection is returned when use nil preferred connection
	ErrNilConnection = errors.New("nil connection")
)
