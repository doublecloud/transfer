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
	table "github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/ydbsql"
)

const BatchSize = 1000

type Command struct {
	table string
	count int
}

type logMessage struct {
	App       string
	Host      string
	Timestamp time.Time
	HTTPCode  uint32
	Message   string
}

func wrap(err error, explanation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", explanation, err)
	}
	return err
}

func getLogBatch(logs []logMessage, offset int) []logMessage {
	logs = logs[:0]
	for i := 0; i < BatchSize; i++ {
		message := logMessage{
			App:       fmt.Sprintf("App_%d", offset%10),
			Host:      fmt.Sprintf("192.168.0.%d", offset%11),
			Timestamp: time.Now().Add(time.Millisecond * time.Duration(i%1000)),
			HTTPCode:  200,
		}
		if i%2 == 0 {
			message.Message = "GET / HTTP/1.1"
		} else {
			message.Message = "GET /images/logo.png HTTP/1.1"
		}

		logs = append(logs, message)
	}
	return logs
}

func (cmd *Command) createLogTable(ctx context.Context, sp table.SessionProvider) error {
	log.Printf("Create table: %v\n", cmd.table)
	return wrap(table.Retry(ctx, sp, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		return session.CreateTable(ctx, cmd.table,
			table.WithColumn("App", ydb.Optional(ydb.TypeUTF8)),
			table.WithColumn("Timestamp", ydb.Optional(ydb.TypeTimestamp)),
			table.WithColumn("Host", ydb.Optional(ydb.TypeUTF8)),
			table.WithColumn("HTTPCode", ydb.Optional(ydb.TypeUint32)),
			table.WithColumn("Message", ydb.Optional(ydb.TypeUTF8)),
			table.WithPrimaryKeyColumn("App", "Timestamp", "Host"),
		)
	})), "failed to create table")
}

func (cmd *Command) writeLogBatch(ctx context.Context, sp table.SessionProvider, logs []logMessage) error {
	return wrap(table.Retry(ctx, sp, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		rows := make([]ydb.Value, 0, len(logs))

		for _, msg := range logs {
			rows = append(rows, ydb.StructValue(
				ydb.StructFieldValue("App", ydb.UTF8Value(msg.App)),
				ydb.StructFieldValue("Host", ydb.UTF8Value(msg.Host)),
				ydb.StructFieldValue("Timestamp", ydbsql.Timestamp(msg.Timestamp).Value()),
				ydb.StructFieldValue("HTTPCode", ydb.Uint32Value(msg.HTTPCode)),
				ydb.StructFieldValue("Message", ydb.UTF8Value(msg.Message)),
			))
		}

		return wrap(session.BulkUpsert(ctx, cmd.table, ydb.ListValue(rows...)),
			"failed to perform bulk upsert")
	})), "failed to write log batch")
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

	if err := cmd.createLogTable(ctx, db.Table().Pool()); err != nil {
		return wrap(err, "failed to create table")
	}
	var logs []logMessage
	for offset := 0; offset < cmd.count; offset++ {
		logs = getLogBatch(logs, offset)
		if err := cmd.writeLogBatch(ctx, db.Table().Pool(), logs); err != nil {
			return wrap(err, fmt.Sprintf("failed to write batch offset %d", offset))
		}
		fmt.Print(".")
	}
	fmt.Print("\n")
	log.Print("Done.\n")

	return nil
}

func (cmd *Command) ExportFlags(_ context.Context, flagSet *flag.FlagSet) {
	flagSet.IntVar(&cmd.count, "count", 1000, "count requests")
	flagSet.StringVar(&cmd.table, "table", "bulk_upsert_example", "Path for table")
}
