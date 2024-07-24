package main

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"text/template"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

type templateConfig struct {
	TablePathPrefix string
}

var fill = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
DECLARE $ordersData AS List<Struct<
	customer_id: Uint64,
	order_id: Uint64,
	description: Utf8,
	order_date: Date>>;
REPLACE INTO orders
SELECT
	customer_id,
	order_id,
	description,
    order_date
FROM AS_TABLE($ordersData);
`))

func prepareScheme(ctx context.Context, db *connect.Connection, prefix string) (err error) {
	err = db.CleanupDatabase(ctx, prefix, "orders")
	if err != nil {
		return err
	}
	err = db.EnsurePathExists(ctx, prefix)
	if err != nil {
		return err
	}

	err = createTables(ctx, db.Table().Pool(), prefix)
	if err != nil {
		return fmt.Errorf("create tables error: %w", err)
	}
	return nil
}

func prepareData(ctx context.Context, db *connect.Connection, prefix string) (err error) {
	err = fillTablesWithData(ctx, db.Table().Pool(), prefix)
	if err != nil {
		return fmt.Errorf("fill tables with data error: %w", err)
	}
	return nil
}

func createTables(ctx context.Context, sessionPool *table.SessionPool, prefix string) (err error) {
	err = table.Retry(ctx, sessionPool,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "orders"),
				table.WithColumn("customer_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("order_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("description", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("order_date", ydb.Optional(ydb.TypeDate)),
				table.WithPrimaryKeyColumn("customer_id", "order_id"),
				//For sorting demonstration
				table.WithProfile(
					table.WithPartitioningPolicy(
						table.WithPartitioningPolicyExplicitPartitions(
							ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(1))),
							ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(2))),
							ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(3))),
						),
					),
				),
			)
		}),
	)
	if err != nil {
		return err
	}
	return nil
}

func fillTablesWithData(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	// Prepare write transaction.
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, render(fill, templateConfig{
				TablePathPrefix: prefix,
			}))
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$ordersData", getSeasonsData()),
			))
			return err
		}),
	)
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func seasonData(customerID, orderID uint64, description string, date uint32) ydb.Value {
	return ydb.StructValue(
		ydb.StructFieldValue("customer_id", ydb.Uint64Value(customerID)),
		ydb.StructFieldValue("order_id", ydb.Uint64Value(orderID)),
		ydb.StructFieldValue("description", ydb.UTF8Value(description)),
		ydb.StructFieldValue("order_date", ydb.DateValue(date)),
	)
}

func getSeasonsData() ydb.Value {
	return ydb.ListValue(
		seasonData(1, 1, "Order 1", days("2006-02-03")),
		seasonData(1, 2, "Order 2", days("2007-08-24")),
		seasonData(1, 3, "Order 3", days("2008-11-21")),
		seasonData(1, 4, "Order 4", days("2010-06-25")),
		seasonData(2, 1, "Order 1", days("2014-04-06")),
		seasonData(2, 2, "Order 2", days("2015-04-12")),
		seasonData(2, 3, "Order 3", days("2016-04-24")),
		seasonData(2, 4, "Order 4", days("2017-04-23")),
		seasonData(2, 5, "Order 5", days("2018-03-25")),
		seasonData(3, 1, "Order 1", days("2019-04-23")),
		seasonData(3, 2, "Order 3", days("2020-03-25")),
	)
}

const DateISO8601 = "2006-01-02"

func days(date string) uint32 {
	t, err := time.Parse(DateISO8601, date)
	if err != nil {
		panic(err)
	}
	return ydb.Time(t).Date()
}

func intToStringDate(orderDate uint32) string {
	date := time.Unix(int64(orderDate)*24*60*60, 0)
	return date.Format(DateISO8601)
}
