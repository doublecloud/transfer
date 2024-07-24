package middlewares

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/base"
)

// Asynchronizer is tasks.asynchronousSnapshotState for abstract2.
// However, as there is no way to tell if a batch contains "non-row" events, push is always asynchronous.
type Asynchronizer struct {
	target base.EventTarget

	errChs []chan error
}

func NewAsynchronizer(target base.EventTarget) *Asynchronizer {
	return &Asynchronizer{
		target: target,

		errChs: make([]chan error, 0),
	}
}

func (s *Asynchronizer) Close() error {
	for _, errCh := range s.errChs {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func (s *Asynchronizer) Push(input base.EventBatch) error {
	var result error = nil
	lastReadChI := 0

overErrChs:
	for _, errCh := range s.errChs {
		select {
		case err := <-errCh:
			lastReadChI += 1
			if err != nil {
				result = err
				break overErrChs
			}
		default:
			break overErrChs
		}
	}
	s.errChs = s.errChs[lastReadChI:]
	if result != nil {
		return result
	}

	s.errChs = append(s.errChs, s.target.AsyncPush(input))
	return nil
}
