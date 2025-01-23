package cli

import (
	"fmt"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/term"
)

var spin = []string{"--", "\\", "|", "/"}

type spinner struct {
	isTty   bool
	width   int
	reverse bool
	iter    int
	close   chan struct{}
	wg      sync.WaitGroup
	delay   time.Duration
}

// NewSpinner creates pause filler in CLI to make it visually alive.
func NewSpinner(width int, delay time.Duration) *spinner {
	if width < 0 {
		width = 3
	} else if width > 30 {
		width = 30
	}

	if delay < 0 {
		delay = 0
	}

	var stdout interface{} = syscall.Stdout
	stdoutFD, ok := stdout.(int)
	isTerminal := term.IsTerminal(stdoutFD)

	return &spinner{
		isTty:   ok && isTerminal,
		width:   width,
		reverse: false,
		iter:    0,
		close:   make(chan struct{}),
		wg:      sync.WaitGroup{},
		delay:   delay,
	}
}

func (s *spinner) clearLine() {
	if s.isTty {
		var sb strings.Builder
		for k := 0; k < s.width; k++ {
			sb.WriteRune(' ')
		}
		fmt.Printf("\r")
		fmt.Print(sb.String())
		fmt.Printf("\r")
	}
}

func (s *spinner) Start() {
	s.wg.Add(1)
	s.close = make(chan struct{})
	go func() {
		defer s.wg.Done()
		for {
			var sb strings.Builder
			indent := (s.iter + 3) / len(spin)
			sp := spin[s.iter%4]
			for k := 0; k < s.width; k++ {
				switch {
				case k == 0:
					sb.WriteRune('[')
				case k == s.width-1:
					sb.WriteRune(']')
				case k-indent >= 0 && k-indent < len(sp):
					sb.WriteByte(sp[k-indent])
				default:
					sb.WriteRune(' ')
				}
			}

			if !s.reverse {
				s.iter++
				if s.iter == (s.width-2)*4 {
					s.reverse = true
				}
			} else {
				s.iter--
				if s.iter == 0 {
					s.reverse = false
				}
			}
			fmt.Print(sb.String())
			time.Sleep(s.delay)
			s.clearLine()
			select {
			case _, ok := <-s.close:
				if !ok {
					return
				}
			default:
			}
		}
	}()
}

func (s *spinner) Stop() {
	close(s.close)
	s.wg.Wait()
}
