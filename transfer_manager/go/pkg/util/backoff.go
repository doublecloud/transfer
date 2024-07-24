package util

import (
	"fmt"
	"time"

	"github.com/doublecloud/tross/library/go/core/log"
)

// BackoffLogger uses level "warn" by default
func BackoffLogger(logger log.Logger, msg string) func(error, time.Duration) {
	return BackoffLoggerWarn(logger, msg)
}

const backoffLoggerMsg string = "Will sleep %s and then retry %s because of an error."

func BackoffLoggerWarn(logger log.Logger, msg string) func(error, time.Duration) {
	return func(err error, sleep time.Duration) {
		logger.Warn(fmt.Sprintf(backoffLoggerMsg, sleep, msg), log.Error(err))
	}
}

func BackoffLoggerDebug(logger log.Logger, msg string) func(error, time.Duration) {
	return func(err error, sleep time.Duration) {
		logger.Debug(fmt.Sprintf(backoffLoggerMsg, sleep, msg), log.Error(err))
	}
}
