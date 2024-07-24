package ydb

import (
	"sync"
)

// wrap sync.WaitGroup to allow for 'wait 1st' logic
type WG interface {
	Done()
	Wait()
	Add(delta int)
}

// Must be used only be reference
type wg struct {
	wg        sync.WaitGroup
	once      sync.Once
	firstDone chan struct{}
}

var _ WG = &sync.WaitGroup{}
var _ WG = &wg{}

func (w *wg) Done() {
	w.once.Do(w.first)
	w.wg.Done()
}

func (w *wg) first() {
	close(w.firstDone)
}

func (w *wg) Wait() {
	w.wg.Wait()
}

func (w *wg) Add(delta int) {
	w.wg.Add(delta)
}

func (w *wg) WaitFirst() {
	<-w.firstDone
}

func newWG() *wg {
	w := new(wg)
	w.firstDone = make(chan struct{})
	return w
}
