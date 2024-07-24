package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"path"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/example/internal/cli"
	table "github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

type Command struct {
	config func(cli.Parameters) *ydb.DriverConfig
	table  string
}

func wrap(err error, explanation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", explanation, err)
	}
	return err
}

func (cmd *Command) testUniformPartitions(ctx context.Context, sp table.SessionProvider) error {
	log.Printf("Create table: %v\n", cmd.table)

	return wrap(table.Retry(ctx, sp, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		err := session.CreateTable(ctx, cmd.table,
			table.WithColumn("key", ydb.Optional(ydb.TypeUint64)),
			table.WithColumn("value", ydb.Optional(ydb.TypeJSON)),
			table.WithPrimaryKeyColumn("key"),

			table.WithProfile(
				table.WithPartitioningPolicy(
					table.WithPartitioningPolicyMode(table.PartitioningAutoSplitMerge),
					table.WithPartitioningPolicyUniformPartitions(4),
				),
			),
		)
		if err != nil {
			return wrap(err, "failed to create table")
		}

		desc, err := session.DescribeTable(ctx, cmd.table, table.WithShardKeyBounds())
		if err != nil {
			return wrap(err, "failed to get table description")
		}
		if len(desc.KeyRanges) != 4 {
			return errors.New("key ranges len is not as expected")
		}

		return nil
	})), "failed to execute operation")
}

func (cmd *Command) testExplicitPartitions(ctx context.Context, sp table.SessionProvider) error {
	log.Printf("Create table: %v\n", cmd.table)

	return wrap(table.Retry(ctx, sp, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		err := session.CreateTable(ctx, cmd.table,
			table.WithColumn("key", ydb.Optional(ydb.TypeUint64)),
			table.WithColumn("value", ydb.Optional(ydb.TypeJSON)),
			table.WithPrimaryKeyColumn("key"),

			table.WithProfile(
				table.WithPartitioningPolicy(
					table.WithPartitioningPolicyExplicitPartitions(
						ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(100))),
						ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(300))),
						ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(400))),
					),
				),
			),
		)
		if err != nil {
			return wrap(err, "failed to create table")
		}

		desc, err := session.DescribeTable(ctx, cmd.table, table.WithShardKeyBounds())
		if err != nil {
			return wrap(err, "failed to get table description")
		}
		if len(desc.KeyRanges) != 4 {
			return errors.New("key ranges len is not as expected")
		}

		return nil
	})), "failed to execute operation")
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(connectCtx, params.ConnectParams)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	tableName := cmd.table
	cmd.table = path.Join(params.Prefix(), cmd.table)

	err = db.CleanupDatabase(ctx, params.Prefix(), tableName)
	if err != nil {
		return err
	}
	err = db.EnsurePathExists(ctx, params.Prefix())
	if err != nil {
		return err
	}

	if err := cmd.testUniformPartitions(ctx, db.Table().Pool()); err != nil {
		return wrap(err, "failed to test uniform partitions")
	}

	err = db.CleanupDatabase(ctx, params.Prefix(), tableName)
	if err != nil {
		return err
	}

	if err := cmd.testExplicitPartitions(ctx, db.Table().Pool()); err != nil {
		return wrap(err, "failed to test explicit partitions")
	}

	return nil
}

func (cmd *Command) ExportFlags(_ context.Context, flagSet *flag.FlagSet) {
	flagSet.StringVar(&cmd.table, "table", "explicit_partitions_example", "Path for table")
}
