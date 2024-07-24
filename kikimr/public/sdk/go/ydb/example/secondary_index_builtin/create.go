package main

import (
	"context"
	"path"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

func doCreate(
	ctx context.Context,
	sp *table.SessionPool,
	prefix string,
	args ...string,
) error {
	for _, desc := range []struct {
		name string
		opts []table.CreateTableOption
	}{
		{
			name: "series",
			opts: []table.CreateTableOption{
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("info", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("release_date", ydb.Optional(ydb.TypeDatetime)),
				table.WithColumn("views", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("uploaded_user_id", ydb.Optional(ydb.TypeUint64)),

				table.WithPrimaryKeyColumn("series_id"),

				table.WithIndex("views_index",
					table.WithIndexType(table.GlobalIndex()),
					table.WithIndexColumns("views"),
				),
				table.WithIndex("users_index",
					table.WithIndexType(table.GlobalIndex()),
					table.WithIndexColumns("uploaded_user_id"),
				),
			},
		},
		{
			name: "users",
			opts: []table.CreateTableOption{
				table.WithColumn("user_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("name", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("age", ydb.Optional(ydb.TypeUint32)),

				table.WithPrimaryKeyColumn("user_id"),

				table.WithIndex("name_index",
					table.WithIndexType(table.GlobalIndex()),
					table.WithIndexColumns("name"),
				),
			},
		},
	} {
		err := table.Retry(ctx, sp,
			table.OperationFunc(func(ctx context.Context, s *table.Session) error {
				return s.CreateTable(ctx, path.Join(prefix, desc.name), desc.opts...)
			}),
		)
		if err != nil {
			return err
		}
	}
	return nil
}
