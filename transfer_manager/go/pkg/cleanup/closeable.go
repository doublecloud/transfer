package cleanup

import (
	"github.com/doublecloud/tross/library/go/core/log"
)

type Closeable interface {
	Close() error
}

func Close(closeable Closeable, logger log.Logger) {
	err := closeable.Close()
	if err != nil {
		logger.Errorf("unable to close %T: %v", closeable, err)
	}
}
