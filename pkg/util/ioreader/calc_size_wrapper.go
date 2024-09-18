package ioreader

import "io"

type CalcSizeWrapper struct {
	ioReader io.Reader
	Counter  uint64
}

func (w *CalcSizeWrapper) Read(p []byte) (int, error) {
	n, err := w.ioReader.Read(p)
	w.Counter += uint64(n)
	return n, err
}

func NewCalcSizeWrapper(ioReader io.Reader) *CalcSizeWrapper {
	return &CalcSizeWrapper{
		ioReader: ioReader,
		Counter:  uint64(0),
	}
}
