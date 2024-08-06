package util

import (
	"io"

	"go.ytsaurus.tech/library/go/core/log"
)

// Rollbacks is cancellable caller of rollback functions.
// Not thread safe.
//
// Use case scenario: transactional initialization
//
//	func NewResource() (*MyResource, err) {
//	   rollbacks := Rollbacks{}
//	   defer rollbacks.Do() // #1 defer call of rollbacks during initialization
//
//	   resource1, err := aquisiteResource1()
//	   rollbacks.add(func() {resource1.Close()} // #2 Closing resource is being put into rollbacks phase
//	   if err != nil {
//	      return nil, err // #3 on exit, all rollbacks functions are being called due to comment #1
//	   }
//	   resource2, err := aquisiteResource2()
//	   rollbacks.add(func() {resource1.Retain()} // #4
//	   if err != nil {
//	      return nil, err // #5 if error occured, both resources in #2 and #4 will be freed on deferred call in #1
//	   }
//
//	   rollbacks.Cancel() // #6 when we come to point where object with resources is correctly initialized, cancel rollbacks
//	   return &MyResource{ resource1, resource2}, nil // when deferred #1 called, no resource free happen due to comment #6
//	}
type Rollbacks struct {
	canceled          bool
	rollbackFunctions []func()
}

func (r *Rollbacks) Add(f func()) {
	r.rollbackFunctions = append(r.rollbackFunctions, f)
}

func (r *Rollbacks) AddCloser(closer io.Closer, logger log.Logger, warningMessage string) {
	r.rollbackFunctions = append(r.rollbackFunctions, func() {
		if err := closer.Close(); err != nil {
			logger.Warnf("%s: %s", warningMessage, err.Error())
		}
	})
}

func (r *Rollbacks) Do() {
	if r.canceled {
		return
	}

	for i := len(r.rollbackFunctions) - 1; i >= 0; i-- {
		r.rollbackFunctions[i]()
	}
}

func (r *Rollbacks) Cancel() {
	r.canceled = true
}
