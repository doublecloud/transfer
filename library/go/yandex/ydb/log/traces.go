package log

import (
	"github.com/doublecloud/tross/library/go/yandex/ydb/ydb-topic-fed-sdk/fedlog"
	"github.com/doublecloud/tross/library/go/yandex/ydb/ydb-topic-fed-sdk/fedtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	ydbLog "github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.ytsaurus.tech/library/go/core/log"
)

type Option = ydbLog.Option

func WithLogQuery() Option {
	return ydbLog.WithLogQuery()
}

func WithTraces(l log.Logger, d trace.Detailer, opts ...Option) ydb.Option {
	a := adapter{l: l}
	return ydb.MergeOptions(
		ydb.WithTraceDriver(ydbLog.Driver(a, d, opts...)),
		ydb.WithTraceTable(ydbLog.Table(a, d, opts...)),
		ydb.WithTraceScripting(ydbLog.Scripting(a, d, opts...)),
		ydb.WithTraceScheme(ydbLog.Scheme(a, d, opts...)),
		ydb.WithTraceCoordination(ydbLog.Coordination(a, d, opts...)),
		ydb.WithTraceRatelimiter(ydbLog.Ratelimiter(a, d, opts...)),
		ydb.WithTraceDiscovery(ydbLog.Discovery(a, d, opts...)),
		ydb.WithTraceTopic(ydbLog.Topic(a, d, opts...)),
		ydb.WithTraceDatabaseSQL(ydbLog.DatabaseSQL(a, d, opts...)),
	)
}

func Table(l log.Logger, d trace.Detailer, opts ...ydbLog.Option) trace.Table {
	return ydbLog.Table(&adapter{l: l}, d, opts...)
}

func Topic(l log.Logger, d trace.Detailer, opts ...ydbLog.Option) trace.Topic {
	return ydbLog.Topic(&adapter{l: l}, d, opts...)
}

func Driver(l log.Logger, d trace.Detailer, opts ...ydbLog.Option) trace.Driver {
	return ydbLog.Driver(&adapter{l: l}, d, opts...)
}

func Coordination(l log.Logger, d trace.Detailer, opts ...ydbLog.Option) trace.Coordination {
	return ydbLog.Coordination(&adapter{l: l}, d, opts...)
}

func Discovery(l log.Logger, d trace.Detailer, opts ...ydbLog.Option) trace.Discovery {
	return ydbLog.Discovery(&adapter{l: l}, d, opts...)
}

func Ratelimiter(l log.Logger, d trace.Detailer, opts ...ydbLog.Option) trace.Ratelimiter {
	return ydbLog.Ratelimiter(&adapter{l: l}, d, opts...)
}

func Scheme(l log.Logger, d trace.Detailer, opts ...ydbLog.Option) trace.Scheme {
	return ydbLog.Scheme(&adapter{l: l}, d, opts...)
}

func Scripting(l log.Logger, d trace.Detailer, opts ...ydbLog.Option) trace.Scripting {
	return ydbLog.Scripting(&adapter{l: l}, d, opts...)
}

func DatabaseSQL(l log.Logger, d trace.Detailer, opts ...ydbLog.Option) trace.DatabaseSQL {
	return ydbLog.DatabaseSQL(&adapter{l: l}, d, opts...)
}

func WithFedDriverTraces(l log.Logger, fedDetails fedtrace.Detailer) fedtrace.FedDriver {
	return fedlog.FedDriver(&adapter{l: l}, fedDetails)
}

func WithFedTopicReaderTraces(l log.Logger, fedDetails fedtrace.Detailer, ydbDetails trace.Detailer) *fedtrace.FedTopicReader {
	return fedlog.FedTopicReader(&adapter{l: l}, fedDetails, ydbDetails)
}

func WithFedTopicWriterTraces(l log.Logger, fedDetails fedtrace.Detailer, ydbDetails trace.Detailer) *fedtrace.FedTopicWriter {
	return fedlog.FedTopicWriter(&adapter{l: l}, fedDetails, ydbDetails)
}
