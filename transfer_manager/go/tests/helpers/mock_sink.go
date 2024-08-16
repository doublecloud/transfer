package helpers

import "github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"

type MockSink struct {
	PushCallback func([]abstract.ChangeItem)
}

func (s *MockSink) Close() error {
	return nil
}

func (s *MockSink) Push(input []abstract.ChangeItem) error {
	s.PushCallback(input)
	return nil
}
