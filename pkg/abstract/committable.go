package abstract

// Committable is a sinker that needs to be completed after snapshot.
type Committable interface {
	Sinker

	// Commit commits all changes made by all sinks during the operation of this transfer.
	// This can be implemented in the future for CH and S3. To implement atomicity, you need to use a transaction that
	// captures all actions with destination DB during the snapshot. This transaction must be committed when calling Commit().
	Commit() error
}
