package abstract

import "io"

type (
	Pusher func(items []ChangeItem) error
)

// Sinker is the destination's data writer interface.
//
// All its methods are guaranteed to be called non-concurrently (synchronously).
//
// TODO: rename to Sink.
type Sinker interface {
	io.Closer
	// Push writes the given items into destination synchronously. If its result is nil, the items are considered to be successfully written to the destination.
	// The method must be retriable: it can be called again after it returns an error, except for fatal errors (these must be wrapped in a particular struct)
	Push(items []ChangeItem) error
}

// TODO: Drop by making transformers a common middleware.
type SinkOption func(sinker Sinker) Sinker
