package scanner

import (
	"bufio"
	"bytes"
	"math"

	"golang.org/x/xerrors"
)

type LineBreakScanner struct {
	scanner *bufio.Scanner
}

func NewLineBreakScanner(data []byte) *LineBreakScanner {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	// in default the size of buffer 64kb
	bufSize := int(math.Min(1*1024*1024, float64(len(data)+2)))
	buf := make([]byte, 0, bufSize)
	scanner.Buffer(buf, bufSize)

	return &LineBreakScanner{
		scanner: scanner,
	}
}

func (s *LineBreakScanner) Scan() bool {
	return s.scanner.Scan()
}

func (s *LineBreakScanner) ScanAll() ([]string, error) {
	var allEvents []string
	for {
		if s.Scan() {
			event, err := s.Event()
			if err != nil {
				return nil, xerrors.Errorf("failed to scan all events: %w", err)
			}
			allEvents = append(allEvents, string(event))
		} else {
			// returned false, if err = nil EOF reached
			err := s.Err()
			if err != nil {
				return nil, xerrors.Errorf("failed to scan all events: %w", err)
			}
			break
		}
	}
	return allEvents, nil
}

func (s *LineBreakScanner) Event() ([]byte, error) {
	event := s.scanner.Bytes()
	if event == nil {
		return nil, xerrors.New("event was not initialized")
	}

	return event, nil
}

func (s *LineBreakScanner) Err() error {
	return s.scanner.Err()
}
