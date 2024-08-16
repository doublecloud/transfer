package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"path"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/example/internal/cli"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
)

const (
	DocTablePartitionCount = 4
	ExpirationQueueCount   = 4
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

	cleanupDBs := []string{"documents"}
	for i := 0; i < ExpirationQueueCount; i++ {
		cleanupDBs = append(cleanupDBs, fmt.Sprintf("expiration_queue_%v", i))
	}

	err = db.CleanupDatabase(ctx, params.Prefix(), cleanupDBs...)
	if err != nil {
		return err
	}
	err = db.EnsurePathExists(ctx, params.Prefix())
	if err != nil {
		return err
	}

	err = createTables(ctx, db.Table().Pool(), params.Prefix())
	if err != nil {
		return fmt.Errorf("create tables error: %w", err)
	}

	err = addDocument(ctx, db.Table().Pool(), params.Prefix(),
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		1)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	err = addDocument(ctx, db.Table().Pool(), params.Prefix(),
		"https://ya.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		2)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	err = readDocument(ctx, db.Table().Pool(), params.Prefix(), "https://yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}
	err = readDocument(ctx, db.Table().Pool(), params.Prefix(), "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	for i := uint64(0); i < ExpirationQueueCount; i++ {
		if err = deleteExpired(ctx, db.Table().Pool(), params.Prefix(), i, 1); err != nil {
			return fmt.Errorf("delete expired failed: %w", err)
		}
	}

	err = readDocument(ctx, db.Table().Pool(), params.Prefix(), "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	err = addDocument(ctx, db.Table().Pool(), params.Prefix(),
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		2)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	err = addDocument(ctx, db.Table().Pool(), params.Prefix(),
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		3)
	if err != nil {
		return fmt.Errorf("add document failed: %w", err)
	}

	for i := uint64(0); i < ExpirationQueueCount; i++ {
		if err = deleteExpired(ctx, db.Table().Pool(), params.Prefix(), i, 2); err != nil {
			return fmt.Errorf("delete expired failed: %w", err)
		}
	}

	err = readDocument(ctx, db.Table().Pool(), params.Prefix(), "https://yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}
	err = readDocument(ctx, db.Table().Pool(), params.Prefix(), "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %w", err)
	}

	return nil
}

func readExpiredBatchTransaction(ctx context.Context, sp *table.SessionPool, prefix string, queue,
	timestamp, prevTimestamp, prevDocID uint64) (*table.Result,
	error) {

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $timestamp AS Uint64;
        DECLARE $prev_timestamp AS Uint64;
        DECLARE $prev_doc_id AS Uint64;

        $data = (
            SELECT *
            FROM expiration_queue_%v
            WHERE
                ts <= $timestamp
                AND
                ts > $prev_timestamp

            UNION ALL

            SELECT *
            FROM expiration_queue_%v
            WHERE
                ts = $prev_timestamp AND doc_id > $prev_doc_id
            ORDER BY ts, doc_id
            LIMIT 100
        );

        SELECT ts, doc_id
        FROM $data
        ORDER BY ts, doc_id
        LIMIT 100;`, prefix, queue, queue)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	var res *table.Result
	err := table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, readTx, table.NewQueryParameters(
				table.ValueParam("$timestamp", ydb.Uint64Value(timestamp)),
				table.ValueParam("$prev_timestamp", ydb.Uint64Value(prevTimestamp)),
				table.ValueParam("$prev_doc_id", ydb.Uint64Value(prevDocID)),
			))
			return err
		}),
	)
	if err != nil {
		return nil, err
	}
	if res.Err() != nil {
		return nil, res.Err()
	}
	return res, nil
}

func deleteDocumentWithTimestamp(ctx context.Context, sp *table.SessionPool, prefix string, queue, lastDocID, timestamp uint64) error {
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $doc_id AS Uint64;
        DECLARE $timestamp AS Uint64;

        DELETE FROM documents
        WHERE doc_id = $doc_id AND ts = $timestamp;

        DELETE FROM expiration_queue_%v
        WHERE ts = $timestamp AND doc_id = $doc_id;`, prefix, queue)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$doc_id", ydb.Uint64Value(lastDocID)),
				table.ValueParam("$timestamp", ydb.Uint64Value(timestamp)),
			))
			return err
		}),
	)
}

func deleteExpired(ctx context.Context, sp *table.SessionPool, prefix string, queue, timestamp uint64) error {
	fmt.Printf("> DeleteExpired from queue #%d:\n", queue)
	empty := false
	lastTimestamp := uint64(0)
	lastDocID := uint64(0)

	for !empty {
		res, err := readExpiredBatchTransaction(ctx, sp, prefix, queue, timestamp, lastTimestamp, lastDocID)
		if err != nil {
			return err
		}

		empty = true
		res.NextSet()
		for res.NextRow() {
			empty = false
			res.SeekItem("doc_id")
			lastDocID = res.OUint64()
			res.SeekItem("ts")
			lastTimestamp = res.OUint64()
			fmt.Printf("\tDocId: %d\n\tTimestamp: %d\n", lastDocID, lastTimestamp)

			err = deleteDocumentWithTimestamp(ctx, sp, prefix, queue, lastDocID, lastTimestamp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func readDocument(ctx context.Context, sp *table.SessionPool, prefix, url string) error {
	fmt.Printf("> ReadDocument \"%v\":\n", url)

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

        DECLARE $url AS Utf8;

        $doc_id = Digest::CityHash($url);

        SELECT doc_id, url, html, ts
        FROM documents
        WHERE doc_id = $doc_id;`, prefix)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	var res *table.Result
	err := table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, readTx, table.NewQueryParameters(
				table.ValueParam("$url", ydb.UTF8Value(url)),
			))
			return err
		}),
	)
	if err != nil {
		return err
	}
	if res.Err() != nil {
		return res.Err()
	}
	if res.NextSet() && res.NextRow() {
		res.SeekItem("doc_id")
		fmt.Printf("\tDocId: %v\n", res.OUint64())

		res.SeekItem("url")
		fmt.Printf("\tUrl: %v\n", res.OUTF8())

		res.SeekItem("ts")
		fmt.Printf("\tTimestamp: %v\n", res.OUint64())

		res.SeekItem("html")
		fmt.Printf("\tHtml: %v\n", res.OUTF8())
	} else {
		fmt.Println("\tNot found")
	}

	return nil
}

func addDocument(ctx context.Context, sp *table.SessionPool, prefix, url, html string, timestamp uint64) error {
	fmt.Printf("> AddDocument: \n\tUrl: %v\n\tTimestamp: %v\n", url, timestamp)

	queue := rand.Intn(ExpirationQueueCount)
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $url AS Utf8;
        DECLARE $html AS Utf8;
        DECLARE $timestamp AS Uint64;

        $doc_id = Digest::CityHash($url);

        REPLACE INTO documents
            (doc_id, url, html, ts)
        VALUES
            ($doc_id, $url, $html, $timestamp);

        REPLACE INTO expiration_queue_%v
            (ts, doc_id)
        VALUES
            ($timestamp, $doc_id);`, prefix, queue)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$url", ydb.UTF8Value(url)),
				table.ValueParam("$html", ydb.UTF8Value(html)),
				table.ValueParam("$timestamp", ydb.Uint64Value(timestamp)),
			))
			return err
		}),
	)
}

func createTables(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "documents"),
				table.WithColumn("doc_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("url", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("html", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("ts", ydb.Optional(ydb.TypeUint64)),
				table.WithPrimaryKeyColumn("doc_id"),
				table.WithProfile(
					table.WithPartitioningPolicy(
						table.WithPartitioningPolicyUniformPartitions(uint64(DocTablePartitionCount)))),
			)
		}),
	)
	if err != nil {
		return err
	}

	for i := 0; i < ExpirationQueueCount; i++ {
		err = table.Retry(ctx, sp,
			table.OperationFunc(func(ctx context.Context, s *table.Session) error {
				return s.CreateTable(ctx, path.Join(prefix, fmt.Sprintf("expiration_queue_%v", i)),
					table.WithColumn("doc_id", ydb.Optional(ydb.TypeUint64)),
					table.WithColumn("ts", ydb.Optional(ydb.TypeUint64)),
					table.WithPrimaryKeyColumn("ts", "doc_id"),
				)
			}),
		)
		if err != nil {
			return err
		}
	}

	return nil
}
