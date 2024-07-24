package fedlog

import (
	"time"

	"github.com/doublecloud/tross/library/go/yandex/ydb/ydb-topic-fed-sdk/fedtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func FedTopicWriter(logger log.Logger, fedDetails fedtrace.Detailer, details trace.Detailer) *fedtrace.FedTopicWriter {
	res := &fedtrace.FedTopicWriter{}
	res.YdbDetails = details
	res.Logger = logger

	res.OnDBWriterStart = func(startInfo fedtrace.FedTopicWriterDBWriterConnectionStartInfo) func(fedtrace.FedTopicWriterDBWriterConnectionDoneInfo) {
		if fedDetails.Details()&fedtrace.FedWriterLifetime == 0 {
			return nil
		}

		start := time.Now()

		ctx := with(*startInfo.Context, log.TRACE, "ydbfed", "writer")
		logger.Log(
			ctx,
			"Start writer to the db",
			log.String("db_id", startInfo.DBInfo.ID),
			log.String("db_name", startInfo.DBInfo.Name),
			log.String("db_path", startInfo.DBInfo.Path),
			log.String("db_endpoint", startInfo.DBInfo.Endpoint),
			log.Int64("db_weight", startInfo.DBInfo.Weight),
			log.Stringer("db_status", startInfo.DBInfo.Status),
			versionField(),
		)

		return func(doneInfo fedtrace.FedTopicWriterDBWriterConnectionDoneInfo) {
			logDone(
				ctx,
				logger,
				doneInfo.Error,
				log.INFO,
				log.ERROR,
				"Stop writer to th db",
				latencyField(start),
				versionField(),
			)
		}
	}

	res.OnDBWriterClose = func(startInfo fedtrace.FedTopicWriterDBWriterDisconnectionStartInfo) func(info fedtrace.FedTopicWriterDBWriterDisconnectionDoneInfo) {
		if fedDetails.Details()&fedtrace.FedWriterLifetime == 0 {
			return nil
		}

		start := time.Now()
		ctx := with(*startInfo.Context, log.TRACE, "ydbfed", "writer")
		logger.Log(
			ctx,
			"Start closing writer",
			versionField(),
		)

		return func(doneInfo fedtrace.FedTopicWriterDBWriterDisconnectionDoneInfo) {
			logDone(
				ctx,
				logger,
				doneInfo.Error,
				log.INFO,
				log.ERROR,
				"close fed writer",
				latencyField(start),
				versionField(),
			)
		}
	}

	res.OnWrite = func(startInfo fedtrace.FedTopicWriterDBWriteStartInfo) func(fedtrace.FedTopicWriterDBWriteDoneInfo) {
		start := time.Now()

		ctx := with(*startInfo.Context, log.TRACE, "ydbfed", "writer")
		logger.Log(
			ctx,
			"Write messages",
			log.Int64("first_seqno", startInfo.FirstMessageSeqNo),
			log.Int64("last_seqno", startInfo.LastMessageSeqNo),
			log.Int("messages_count", startInfo.MessagesCount),
			versionField(),
		)

		return func(doneInfo fedtrace.FedTopicWriterDBWriteDoneInfo) {
			logDone(
				ctx,
				logger,
				doneInfo.Error,
				log.INFO,
				log.ERROR,
				"Write message finished",
				log.Int64("first_seqno", startInfo.FirstMessageSeqNo),
				log.Int64("last_seqno", startInfo.LastMessageSeqNo),
				log.Int("messages_count", startInfo.MessagesCount),
				latencyField(start),
				versionField(),
			)
		}
	}

	return res
}
