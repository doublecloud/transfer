package abstract

type Closeable interface {
	// Must be safe for concurrent use
	Close()
}

// io.Closer has an interface:
//     Close() error
// Closeable has an interface:
//     Close()

type EmptyIoCloser struct{} // stub implementation of io.Closer

func (s *EmptyIoCloser) Close() error {
	return nil
}

func NewEmptyIoCloser() *EmptyIoCloser {
	return &EmptyIoCloser{}
}
