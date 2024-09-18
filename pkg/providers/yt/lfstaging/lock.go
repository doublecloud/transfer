package lfstaging

import (
	"context"
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytlock"
	"golang.org/x/xerrors"
)

type lock struct {
	path ypath.Path
	yc   yt.Client
}

func newLock(yc yt.Client, path ypath.Path) *lock {
	return &lock{
		path: path,
		yc:   yc,
	}
}

func (l *lock) WithLock(fn func() error) error {
	lock := ytlock.NewLock(l.yc, l.path)
	_, err := lock.Acquire(context.Background())
	if err != nil {
		time.Sleep(10 * time.Minute)
		return xerrors.Errorf("Cannot acquire lock: %w", err)
	}
	if err := fn(); err != nil {
		return err
	}
	err = lock.Release(context.Background())
	if err != nil {
		return xerrors.Errorf("Cannot release lock: %w", err)
	}
	return nil
}
