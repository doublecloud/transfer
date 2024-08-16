package fedlog

import (
	"time"

	"github.com/doublecloud/transfer/library/go/yandex/ydb/ydb-topic-fed-sdk/fedtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func FedTopicReader(logger log.Logger, fedDetails fedtrace.Detailer, details trace.Detailer) *fedtrace.FedTopicReader {
	res := &fedtrace.FedTopicReader{}
	res.YdbDetails = details

	res.OnDBReaderStart = func(startInfo fedtrace.FedTopicReaderDBreaderStartStartInfo) {
		if fedDetails.Details()&fedtrace.FedReaderLifetime == 0 {
			return
		}

		ctx := with(*startInfo.Context, log.TRACE, "ydbfed", "reader")

		logger.Log(
			ctx,
			"Federation reader starting...",
			versionField(),
		)
	}
	res.OnDBReaderClose = func(startInfo fedtrace.FedTopicReaderDBCloseStartInfo) func(info fedtrace.FedTopicReaderDBCloseDoneInfo) {
		if fedDetails.Details()&fedtrace.FedReaderLifetime == 0 {
			return nil
		}

		start := time.Now()
		ctx := with(*startInfo.Context, log.TRACE, "ydbfed", "reader")

		logger.Log(ctx, "Federation reader closing started...")

		return func(doneInfo fedtrace.FedTopicReaderDBCloseDoneInfo) {
			logDone(
				ctx,
				logger,
				doneInfo.Err,
				log.INFO,
				log.ERROR,
				"federation reader close",
				latencyField(start),
				versionField(),
			)
		}
	}

	return res
}
