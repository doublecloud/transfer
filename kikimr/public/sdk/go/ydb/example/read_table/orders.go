package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/example/internal/cli"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
)

type Command struct {
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(connectCtx, params.ConnectParams)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	tableName := "orders"
	fmt.Println("Read whole table, unsorted:")
	err = readTable(ctx, db.Table().Pool(), path.Join(params.Prefix(), tableName))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Sorted by composite primary key:")
	err = readTable(ctx, db.Table().Pool(), path.Join(params.Prefix(), tableName), table.ReadOrdered())
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Any five rows:")
	err = readTable(ctx, db.Table().Pool(), path.Join(params.Prefix(), tableName), table.ReadRowLimit(5))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("First five rows by PK (ascending) with subset of columns:")
	err = readTable(ctx, db.Table().Pool(), path.Join(params.Prefix(), tableName), table.ReadRowLimit(5), table.ReadColumn("customer_id"),
		table.ReadColumn("order_id"),
		table.ReadColumn("order_date"), table.ReadOrdered())
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Read all rows with first PK component (customer_id,) greater or equal than 2 and less then 3:")
	keyRange := table.KeyRange{
		From: ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(2))),
		To:   ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(3))),
	}
	err = readTable(ctx, db.Table().Pool(), path.Join(params.Prefix(), tableName), table.ReadKeyRange(keyRange))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Read all rows with composite PK lexicographically less or equal than (1,4):")
	err = readTable(ctx, db.Table().Pool(), path.Join(params.Prefix(), tableName), table.ReadLessOrEqual(ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(1)), ydb.OptionalValue(ydb.Uint64Value(4)))))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Read all rows with composite PK lexicographically greater or equal than (1,2) and less than (3,4):")
	keyRange = table.KeyRange{
		From: ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(1)), ydb.OptionalValue(ydb.Uint64Value(2))),
		To:   ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(3)), ydb.OptionalValue(ydb.Uint64Value(1))),
	}
	err = readTable(ctx, db.Table().Pool(), path.Join(params.Prefix(), tableName), table.ReadKeyRange(keyRange))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	return nil
}

type row struct {
	id          uint64
	orderID     uint64
	date        time.Time
	description string
}

func readTable(ctx context.Context, sp *table.SessionPool, path string, opts ...table.ReadTableOption) (err error) {
	var res *table.Result

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.StreamReadTable(ctx, path, opts...)
			return err
		}),
	)
	if err != nil {
		return err
	}
	r := row{}
	for res.NextStreamSet(ctx) {
		for res.NextRow() {
			if res.ColumnCount() == 4 {
				description := ""
				err = res.ScanWithDefaults(&r.id, &r.orderID, &description, &r.date)
				if err != nil {
					return err
				}
				log.Printf("#  Order, CustomerId: %d, OrderId: %d, Description: %s, Order date: %s", r.id, r.orderID, description, r.date.Format("2006-01-02"))
			} else {
				err = res.ScanWithDefaults(&r.id, &r.orderID, &r.date)
				if err != nil {
					return err
				}
				log.Printf("#  Order, CustomerId: %d, OrderId: %d, Order date: %s", r.id, r.orderID, r.date.Format("2006-01-02"))
			}
		}
	}
	// check that no errors occur during the scan
	if res.Err() != nil {
		return res.Err()
	}
	return nil
}
