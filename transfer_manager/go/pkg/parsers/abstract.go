package parsers

import (
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/abstract"
)

type Parser interface {
	Do(msg persqueue.ReadMessage, partition abstract.Partition) []abstract.ChangeItem
	DoBatch(batch persqueue.MessageBatch) []abstract.ChangeItem
}

// WrappedParser parser can be layered by wrapping them in extra layers.
// For wrapped parsers we should add extra method for extracting actual parser
type WrappedParser interface {
	Parser
	Unwrap() Parser
}

type AbstractParserConfig interface {
	IsNewParserConfig()
	IsAppendOnly() bool
	Validate() error
}
