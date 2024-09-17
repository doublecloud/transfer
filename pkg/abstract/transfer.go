package abstract

type Transfer interface {
	// Start the transfer. Retry endlessly when errors occur, until stopped with Stop().
	// This method does not block, the work is done in the background.
	Start()

	// Stop the transfer. May be called multiple times even without prior Start() to clean
	// up external resources, e.g. terminate YT operations. Synchronous, i.e. blocks
	// until either all resources are released or an error occurrs.
	Stop() error

	Runtime() Runtime

	Error() error
}
