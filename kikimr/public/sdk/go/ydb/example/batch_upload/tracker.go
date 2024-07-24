package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

type result struct {
	err    error
	pushed int
	failed int
}

type tracker struct {
	cap    int
	pushed int
	failed int
	done   chan error
	stop   chan struct{}
	track  chan result
}

func initTracker(cap int, infly int) *tracker {
	t := tracker{
		cap:    cap,
		pushed: 0,
		failed: 0,
		done:   make(chan error),
		stop:   make(chan struct{}),
		track:  make(chan result, infly),
	}

	go func() {
		defer close(t.done)
		var err error
	loop:
		for {
			select {
			case track := <-t.track:
				if track.err != nil {
					err = track.err
				}
				t.pushed += track.pushed
				t.failed += track.failed
				if t.pushed+t.failed >= t.cap {
					break loop
				}
			case <-t.stop:
				break loop
			}
		}
		t.done <- err
	}()
	return &t
}

func (t *tracker) report() {
	if t.pushed > 0 {
		fmt.Printf("Success on %v of %v\n", t.pushed, t.cap)
	}
	if t.failed > 0 {
		fmt.Printf("Failed on %v of %v\n", t.failed, t.cap)
	}
}

func (t *tracker) respondSignal(s syscall.Signal) {
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, s)
		for range ch {
			t.report()
		}
	}()
}
