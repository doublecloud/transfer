package abstract

import "context"

// Movable is a sinker which can move tables. This interface allows to use temporator middleware.
type Movable interface {
	Sinker
	// Move moves (renames) the given source table into the given destination table
	Move(ctx context.Context, src, dst TableID) error
}
