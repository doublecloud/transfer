package connect

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/scheme"
)

type Connection struct {
	driverConfig *ydb.DriverConfig
	driver       ydb.Driver
	credentials  ydb.Credentials
	table        *tableWrapper
	scheme       *schemeWrapper
}

func (c *Connection) Close() {
	_ = c.table.Pool().Close(context.Background())
	_ = c.driver.Close()
}

func (c *Connection) Table() *tableWrapper {
	return c.table
}

func (c *Connection) Scheme() *scheme.Client {
	return c.scheme.client
}

func (c *Connection) Driver() ydb.Driver {
	return c.driver
}

func (c *Connection) EnsurePathExists(ctx context.Context, path string) error {
	for i := len(c.driverConfig.Database); i < len(path); i++ {
		x := strings.IndexByte(path[i:], '/')
		if x == -1 {
			x = len(path[i:]) - 1
		}
		i += x
		sub := path[:i+1]
		info, err := c.Scheme().DescribePath(ctx, sub)
		operr, ok := err.(*ydb.OpError)
		if ok && operr.Reason == ydb.StatusSchemeError {
			err = c.Scheme().MakeDirectory(ctx, sub)
		}
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		switch info.Type {
		case
			scheme.EntryDatabase,
			scheme.EntryDirectory:
			// OK
		default:
			return fmt.Errorf(
				"entry %q exists but it is a %s",
				sub, info.Type,
			)
		}
	}

	return nil
}

func (c *Connection) CleanupDatabase(ctx context.Context, prefix string, names ...string) error {
	filter := make(map[string]struct{}, len(names))
	for _, n := range names {
		filter[n] = struct{}{}
	}
	var list func(int, string) error
	list = func(i int, p string) error {
		dir, err := c.Scheme().ListDirectory(ctx, p)
		operr, ok := err.(*ydb.OpError)
		if ok && operr.Reason == ydb.StatusSchemeError {
			return nil
		}
		if err != nil {
			return err
		}
		for _, child := range dir.Children {
			if _, has := filter[child.Name]; !has {
				continue
			}
			pt := path.Join(p, child.Name)
			switch child.Type {
			case scheme.EntryDirectory:
				if err := list(i+1, pt); err != nil {
					return err
				}
				if err := c.Scheme().RemoveDirectory(ctx, pt); err != nil {
					return err
				}

			case scheme.EntryTable:
				s, err := c.Table().Pool().Get(ctx)
				if err != nil {
					return err
				}
				err = s.DropTable(ctx, pt)
				_ = c.Table().Pool().Put(ctx, s)
				if err != nil {
					return err
				}

			default:

			}
		}
		return nil
	}
	return list(0, prefix)
}
