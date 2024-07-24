package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/example/internal/cli"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

var (
	simpleCreateQuery = `
PRAGMA TablePathPrefix("%s");
CREATE TABLE small_table (
    a Uint64,
    b Uint64,
    c Utf8,
	d Date,
    PRIMARY KEY (a, b)
);
`
	familyCreateQuery = `
PRAGMA TablePathPrefix("%s");
CREATE TABLE small_table2 (
	a Uint64,
    b Uint64,
    c Utf8 FAMILY family_large,
	d Date,
    PRIMARY KEY (a, b),
    FAMILY family_large (
        COMPRESSION = "lz4"
    )
);
`
	settingsCreateQuery = `
PRAGMA TablePathPrefix("%s");
CREATE TABLE small_table3 (
	a Uint64,
    b Uint64,
    c Utf8,
	d Date,
    PRIMARY KEY (a, b)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED, --Automatic positioning mode by the size of the partition
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 512, --Preferred size of each partition in megabytes
	AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 32 --The minimum number of partitions when the automatic merging of partitions stops working
);
`
	dropQuery = `
PRAGMA TablePathPrefix("%s");
DROP  TABLE small_table;
DROP  TABLE small_table2;
DROP  TABLE small_table3;
`
	alterQuery = `
PRAGMA TablePathPrefix("%s");
ALTER TABLE small_table ADD COLUMN e Uint64, DROP COLUMN c;
`
	alterSettingsQuery = `
PRAGMA TablePathPrefix("%s");
ALTER TABLE small_table2 SET (AUTO_PARTITIONING_BY_SIZE = DISABLED);
`
	alterTTLQuery = `
PRAGMA TablePathPrefix("%s");
ALTER TABLE small_table3 SET (TTL = Interval("PT3H") ON d);
`
)

type Command struct {
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}

func executeQuery(ctx context.Context, sp *table.SessionPool, prefix string, query string) (err error) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			err := s.ExecuteSchemeQuery(ctx, fmt.Sprintf(query, prefix))
			return err
		}),
	)
	if err != nil {
		return err
	}
	return nil
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(connectCtx, params.ConnectParams)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	//simple creation with composite primary key
	err = executeQuery(ctx, db.Table().Pool(), params.Prefix(), simpleCreateQuery)
	if err != nil {
		return err
	}

	//creation with column family
	err = executeQuery(ctx, db.Table().Pool(), params.Prefix(), familyCreateQuery)
	if err != nil {
		return err
	}

	//creation with table settings
	err = executeQuery(ctx, db.Table().Pool(), params.Prefix(), settingsCreateQuery)
	if err != nil {
		return err
	}

	//add column and drop column.
	err = executeQuery(ctx, db.Table().Pool(), params.Prefix(), alterQuery)
	if err != nil {
		return err
	}

	//change AUTO_PARTITIONING_BY_SIZE setting.
	err = executeQuery(ctx, db.Table().Pool(), params.Prefix(), alterSettingsQuery)
	if err != nil {
		return err
	}

	//add TTL. Clear the old data after the three-hour interval has expired.
	err = executeQuery(ctx, db.Table().Pool(), params.Prefix(), alterTTLQuery)
	if err != nil {
		return err
	}

	//drop tables small_table,small_table2,small_table3.
	err = executeQuery(ctx, db.Table().Pool(), params.Prefix(), dropQuery)
	if err != nil {
		return err
	}

	return nil
}
