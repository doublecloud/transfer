package fedtrace

import (
	"context"

	"github.com/doublecloud/transfer/library/go/yandex/ydb/ydb-topic-fed-sdk/fedtypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//go:generate ./gtrace.sh

type Detailer interface {
	Details() Details
}

type Details uint64

// Details implement Detailer interface
func (d Details) Details() Details {
	return d
}

const (
	Discovery Details = 1 << iota // for bitmask: 1, 2, 4, 8, 16, 32, ...
	ClusterConnection

	FedReaderLifetime
	FedReader = FedReaderLifetime

	FedWriterLifetime
	FedWriterMessages
	FedWriter = FedReaderLifetime | FedWriterMessages

	DetailsAll = ^Details(0) // All bits enabled
)

type (
	// gtrace:gen
	FedDriver struct {
		OnDiscovery     func(FedDriverDiscoveryStartInfo) func(FedDriverDiscoveryDoneInfo)
		OnDiscoveryStop func(FedDriverDiscoveryStopInfo)
		OnConnect       func(FedDriverClusterConnectionStartInfo) func(FedDriverClusterConnectionDoneInfo)
	}

	FedDriverDiscoveryStartInfo struct {
		Context   *context.Context
		OldDBInfo []fedtypes.DatabaseInfo
	}
	FedDriverDiscoveryDoneInfo struct {
		DBInfo []fedtypes.DatabaseInfo
		Error  error
	}

	FedDriverDiscoveryStopInfo struct {
		Context    *context.Context
		LastDBInfo []fedtypes.DatabaseInfo
		Reason     error
	}

	FedDriverClusterConnectionStartInfo struct {
		Context *context.Context
		DBInfo  fedtypes.DatabaseInfo
	}
	FedDriverClusterConnectionDoneInfo struct {
		Error error
	}

	// gtrace:gen
	FedTopicReader struct {
		Logger     log.Logger
		YdbDetails trace.Detailer

		OnDBReaderStart func(FedTopicReaderDBreaderStartStartInfo)
		OnDBReaderClose func(FedTopicReaderDBCloseStartInfo) func(info FedTopicReaderDBCloseDoneInfo)
	}

	FedTopicReaderDBreaderStartStartInfo struct {
		Context *context.Context
	}

	FedTopicReaderDBCloseStartInfo struct {
		Context *context.Context
	}

	FedTopicReaderDBCloseDoneInfo struct {
		Err error
	}

	// gtrace:gen
	FedTopicWriter struct {
		Logger     log.Logger
		YdbDetails trace.Detailer

		OnDBWriterStart func(FedTopicWriterDBWriterConnectionStartInfo) func(FedTopicWriterDBWriterConnectionDoneInfo)
		OnDBWriterClose func(FedTopicWriterDBWriterDisconnectionStartInfo) func(info FedTopicWriterDBWriterDisconnectionDoneInfo)
		OnWrite         func(FedTopicWriterDBWriteStartInfo) func(FedTopicWriterDBWriteDoneInfo)
	}

	FedTopicWriterDBWriterConnectionStartInfo struct {
		Context *context.Context
		DBInfo  fedtypes.DatabaseInfo
	}

	FedTopicWriterDBWriterConnectionDoneInfo struct {
		Error error
	}

	FedTopicWriterDBWriterDisconnectionStartInfo struct {
		Context *context.Context
		Reason  error
	}

	FedTopicWriterDBWriterDisconnectionDoneInfo struct {
		Error error
	}

	FedTopicWriterDBWriteStartInfo struct {
		Context           *context.Context
		FirstMessageSeqNo int64
		LastMessageSeqNo  int64
		MessagesCount     int
	}

	FedTopicWriterDBWriteDoneInfo struct {
		Error error
	}
)
