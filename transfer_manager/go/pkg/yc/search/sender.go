package search

import (
	"context"
	"sync"

	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
)

var (
	sender ResourceSender
	mu     sync.RWMutex
)

type Searchable interface {
	SearchResource() (*SearchResource, error)
}

type ResourceSender interface {
	Send(ctx context.Context, res *SearchResource) error
	Close() error
}

// InitSender can be called multiple times
func InitSender(ctx context.Context, searchConf config.Search) (close func(), err error) {
	mu.Lock()
	defer mu.Unlock()

	if sender != nil {
		return func() {}, nil
	}

	sr, err := newResourceSender(ctx, searchConf)
	if err != nil {
		return nil, xerrors.Errorf("newResourceSender error: %v", err)
	}

	sender = sr
	logger.Log.Info("search service initialised")
	return closeSender, nil
}

func InitTestSender(sr ResourceSender) (close func(), err error) {
	mu.Lock()
	defer mu.Unlock()

	sender = sr

	return func() {
		mu.Lock()
		defer mu.Unlock()

		sender = nil
	}, nil
}

func newResourceSender(ctx context.Context, searchConf config.Search) (ResourceSender, error) {
	switch conf := searchConf.(type) {
	case *config.SearchEnabled:
		lbConfig, ok := conf.Writer.(*SearchLogbroker)
		if !ok {
			return nil, xerrors.New("can't cast writer config to lb config")
		}

		sr, err := newLogbrokerSender(lbConfig)
		if err != nil {
			return nil, xerrors.Errorf("newLogbrokerSender error: %v", err)
		}

		err = sr.init(context.Background())
		if err != nil {
			return nil, xerrors.Errorf("sender init error: %v", err)
		}

		return sr, nil
	case *config.SearchDisabled:
		return &DisabledSender{}, nil
	default:
		return nil, xerrors.New("search service config was not specifyed correctly")
	}
}

func SendResource(ctx context.Context, searchable Searchable) error {
	mu.RLock()
	defer mu.RUnlock()

	if sender == nil {
		return nil
	}

	resource, err := searchable.SearchResource()
	if err != nil {
		return xerrors.Errorf("searchable error: %v", err)
	}

	if err := sender.Send(ctx, resource); err != nil {
		return xerrors.Errorf("send error: %v", err)
	}

	return nil
}

func closeSender() {
	mu.Lock()
	defer mu.Unlock()

	if sender == nil {
		logger.Log.Error("sender is nil")
		return
	}

	if err := sender.Close(); err != nil {
		logger.Log.Error("closing sender error", log.Error(err))
	}

	sender = nil
}
