package ydb

import (
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/library/go/core/log"
)

type LogTrace struct {
	log log.Logger
}

func NewLogTrace(logger log.Logger) *LogTrace {
	return &LogTrace{
		log: logger,
	}
}

func (m *LogTrace) OnDialStart(data ydb.DialStartInfo) {
	m.log.Trace("dial start", log.String("address", data.Address))
}

func (m *LogTrace) OnDialDone(data ydb.DialDoneInfo) {
	if data.Error != nil {
		m.log.Warn("dial failed", log.String("data", data.Address), log.Error(data.Error))
	} else {
		m.log.Trace("dial done", log.String("data", data.Address))
	}
}

func (m *LogTrace) OnDiscoveryStart(data ydb.DiscoveryStartInfo) {
	m.log.Trace("discovery started")
}

func (m *LogTrace) OnDiscoveryDone(data ydb.DiscoveryDoneInfo) {
	if data.Error != nil {
		m.log.Warn("discovery failed", log.Error(data.Error))
	} else {
		m.log.Trace("discovery done", log.Any("endpoints", data.Endpoints))
	}
}

func (m *LogTrace) OnOpStart(data ydb.OperationStartInfo) {
	m.log.Trace("operation started", log.String("method", string(data.Method)), log.String("address", data.Address))
}

func (m *LogTrace) OnOpWait(data ydb.OperationWaitInfo) {
	m.log.Trace("waiting for operation",
		log.String("method", string(data.Method)),
		log.String("address", data.Address),
		log.String("operation_id", data.OpID),
	)
}

func (m *LogTrace) OnOpDone(data ydb.OperationDoneInfo) {
	if data.Error != nil {
		m.log.Warn("operation done",
			log.String("method", string(data.Method)),
			log.String("address", data.Address),
			log.String("operation_id", data.OpID),
			log.Any("issues", data.Issues),
			log.Error(data.Error),
		)
	} else {
		m.log.Trace("operation done",
			log.String("method", string(data.Method)),
			log.String("address", data.Address),
			log.String("operation_id", data.OpID),
			log.Any("issues", data.Issues),
		)
	}
}

func (m *LogTrace) YDBTrace() ydb.DriverTrace {
	return ydb.DriverTrace{
		OnDial: func(info ydb.DialStartInfo) func(ydb.DialDoneInfo) {
			m.OnDialStart(info)
			return m.OnDialDone
		},
		OnDiscovery: func(info ydb.DiscoveryStartInfo) func(ydb.DiscoveryDoneInfo) {
			m.OnDiscoveryStart(info)
			return m.OnDiscoveryDone
		},
		OnOperation: func(info ydb.OperationStartInfo) func(ydb.OperationDoneInfo) {
			m.OnOpStart(info)
			return m.OnOpDone
		},
		OnOperationWait: m.OnOpWait,
	}
}
