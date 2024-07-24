package fedtypes

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

var (
	errFedConnClosed = errors.New("ydb topic fed: the driver is closed")
)

type FedConnection struct {
	DBInfo DatabaseInfo

	connect     func(ctx context.Context, info DatabaseInfo) (*ydb.Driver, error)
	m           sync.Mutex
	closeReason error
	driver      *ydb.Driver
}

func NewFedConnectionLazy(dbInfo DatabaseInfo, connect func(ctx context.Context, info DatabaseInfo) (*ydb.Driver, error)) *FedConnection {
	return &FedConnection{
		DBInfo:  dbInfo,
		connect: connect,
	}
}

func NewFedConnectionWithDriver(dbInfo DatabaseInfo, driver *ydb.Driver) *FedConnection {
	return &FedConnection{
		DBInfo: dbInfo,
		driver: driver,
	}
}

func (fc *FedConnection) Driver(ctx context.Context) (*ydb.Driver, error) {
	fc.m.Lock()
	defer fc.m.Unlock()

	if fc.driver != nil {
		return fc.driver, nil
	}

	if fc.closeReason != nil {
		return nil, fc.closeReason
	}

	driver, err := fc.connect(ctx, fc.DBInfo)
	if err == nil {
		fc.driver = driver
	}

	return driver, err
}

// DriverWithoutConnection fast return current driver, may be nil
func (fc *FedConnection) DriverWithoutConnection() *ydb.Driver {
	fc.m.Lock()
	defer fc.m.Unlock()
	return fc.driver
}

func (fc *FedConnection) Close(ctx context.Context, reason error) error {
	fc.m.Lock()
	defer fc.m.Unlock()

	if fc.closeReason != nil {
		return fmt.Errorf("%w: %w", errFedConnClosed, fc.closeReason)
	}

	fc.closeReason = reason

	if fc.driver == nil {
		return nil
	}

	driver := fc.driver
	fc.driver = nil

	return driver.Close(ctx)
}
