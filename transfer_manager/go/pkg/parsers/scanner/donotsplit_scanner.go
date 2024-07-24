package scanner

import "golang.org/x/xerrors"

type DoNotSplitScanner struct {
	data  []byte
	event []byte
}

func NewDoNotSplitScanner(data []byte) *DoNotSplitScanner {
	return &DoNotSplitScanner{
		data:  data,
		event: nil,
	}
}

func (s *DoNotSplitScanner) Scan() bool {
	if len(s.data) == 0 {
		return false
	}

	s.event = s.data
	s.data = s.data[len(s.data):]
	return true
}

func (s *DoNotSplitScanner) Event() ([]byte, error) {
	if s.event == nil {
		return nil, xerrors.New("event was not initialized")
	}

	return s.event, nil
}

func (s *DoNotSplitScanner) Err() error {
	return nil
}
