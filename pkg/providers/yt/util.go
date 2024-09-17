package yt

import (
	"context"
	"time"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/sync/semaphore"
)

var (
	defaultHandleParams = NewHandleParams(50)
)

type nodeHandler func(ctx context.Context, client yt.Client, path ypath.Path, attrs *NodeAttrs) error

func handleNodes(
	ctx context.Context,
	client yt.Client,
	path ypath.Path,
	params *HandleParams,
	handler nodeHandler,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errors := make(chan error)
	var count int
	semaphore := semaphore.NewWeighted(params.ConcurrencyLimit)
	err := handleNodesAsync(ctx, client, path, handler, semaphore, errors, &count)
	if err != nil {
		return err
	}

	for i := 0; i < count; i++ {
		err = <-errors
		if err != nil {
			return xerrors.Errorf("unable to handle node: %w", err)
		}
	}

	return nil
}

func handleNodesAsync(
	ctx context.Context,
	client yt.Client,
	path ypath.Path,
	handler nodeHandler,
	semaphore *semaphore.Weighted,
	errors chan<- error,
	count *int,
) error {
	attrs := new(NodeAttrs)
	if err := client.GetNode(ctx, path.Attrs(), attrs, nil); err != nil {
		return xerrors.Errorf("unable to get node: %w", err)
	}

	switch attrs.Type {
	case yt.NodeMap:
		var childNodes []struct {
			Name string `yson:",value"`
		}
		err := client.ListNode(ctx, path, &childNodes, nil)
		if err != nil {
			return xerrors.Errorf("unable to list nodes: %w", err)
		}
		for _, childNode := range childNodes {
			err = handleNodesAsync(ctx, client, SafeChild(path, childNode.Name), handler, semaphore, errors, count)
			if err != nil {
				return xerrors.Errorf("unable to handle child node: %w", err)
			}
		}
		return nil
	default:
		go func() {
			err := semaphore.Acquire(ctx, 1)
			if err == nil {
				err = handler(ctx, client, path, attrs)
				semaphore.Release(1)
			}
			errors <- err
		}()
		*count++
		return nil
	}
}

type HandleParams struct {
	ConcurrencyLimit int64
}

func NewHandleParams(concurrencyLimit int64) *HandleParams {
	return &HandleParams{ConcurrencyLimit: concurrencyLimit}
}

func UnmountAndWaitRecursive(ctx context.Context, logger log.Logger, client yt.Client, path ypath.Path, params *HandleParams) error {
	if params == nil {
		params = defaultHandleParams
	}
	return handleNodes(ctx, client, path, params,
		func(ctx context.Context, client yt.Client, path ypath.Path, attrs *NodeAttrs) error {
			if attrs.Type == yt.NodeTable && attrs.Dynamic {
				if attrs.TabletState != yt.TabletUnmounted {
					err := migrate.UnmountAndWait(ctx, client, path)
					if err == nil {
						logger.Info("successfully unmounted table", log.Any("path", path))
					}
					return err
				}
				logger.Info("table is already unmounted", log.Any("path", path))
			}
			return nil
		})
}

func MountAndWaitRecursive(ctx context.Context, logger log.Logger, client yt.Client, path ypath.Path, params *HandleParams) error {
	if params == nil {
		params = defaultHandleParams
	}
	return handleNodes(ctx, client, path, params,
		func(ctx context.Context, client yt.Client, path ypath.Path, attrs *NodeAttrs) error {
			if attrs.Type == yt.NodeTable && attrs.Dynamic {
				if attrs.TabletState != yt.TabletMounted {
					err := migrate.MountAndWait(ctx, client, path)
					if err == nil {
						logger.Info("successfully mounted table", log.Any("path", path))
					}
					return err
				}
				logger.Info("table is already mounted", log.Any("path", path))
			}
			return nil
		})
}

func YTColumnToColSchema(columns []schema.Column) *abstract.TableSchema {
	tableSchema := make([]abstract.ColSchema, len(columns))

	for i, c := range columns {
		tableSchema[i] = abstract.ColSchema{
			ColumnName:   c.Name,
			DataType:     string(c.Type),
			PrimaryKey:   c.SortOrder != "",
			Required:     c.Required,
			TableSchema:  "",
			TableName:    "",
			Path:         "",
			FakeKey:      false,
			Expression:   "",
			OriginalType: "",
			Properties:   nil,
		}
	}

	return abstract.NewTableSchema(tableSchema)
}

func WaitMountingPreloadState(yc yt.Client, path ypath.Path) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute) // mounting for reading takes 5-10 minutes
	defer cancel()

	poll := func() (bool, error) {
		var currentState string
		if err := yc.GetNode(ctx, path.Attr("preload_state"), &currentState, nil); err != nil {
			return false, err
		}
		if currentState == "complete" {
			return true, nil
		}

		return false, nil
	}

	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		stop, err := poll()
		if err != nil {
			return xerrors.Errorf("unable to poll master: %w", err)
		}

		if stop {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
