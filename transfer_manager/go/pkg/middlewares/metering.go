package middlewares

import (
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/metering"
)

func InputDataMetering() func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newInputDataMetering(s)
	}
}

func OutputDataMetering() func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newOutputDataMetering(s)
	}
}

type inputDataMetering struct {
	sink abstract.Sinker
}

func newInputDataMetering(s abstract.Sinker) *inputDataMetering {
	return &inputDataMetering{
		sink: s,
	}
}

func (m *inputDataMetering) Close() error {
	return m.sink.Close()
}

func (m *inputDataMetering) Push(input []abstract.ChangeItem) error {
	pushErr := m.sink.Push(input)
	if pushErr == nil {
		metering.Agent().CountInputRows(input)
	}
	return pushErr
}

type outputDataMetering struct {
	sink abstract.Sinker
}

func newOutputDataMetering(s abstract.Sinker) *outputDataMetering {
	return &outputDataMetering{
		sink: s,
	}
}

func (m *outputDataMetering) Close() error {
	return m.sink.Close()
}

func (m *outputDataMetering) Push(input []abstract.ChangeItem) error {
	pushErr := m.sink.Push(input)
	if pushErr == nil {
		metering.Agent().CountOutputRows(input)
	}
	return pushErr
}
