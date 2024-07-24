package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/example/internal/cli"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/opt"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

var (
	roTX = table.TxControl(
		table.BeginTx(table.WithOnlineReadOnly()),
		table.CommitTx(),
	)
	rwTX = table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)
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

	session, err := db.Table().CreateSession(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = session.Close(context.Background())
	}()

	err = db.CleanupDatabase(ctx, params.Prefix(), "users")
	if err != nil {
		return err
	}
	err = db.EnsurePathExists(ctx, params.Prefix())
	if err != nil {
		return err
	}

	err = table.Retry(ctx, table.SingleSession(session),
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(params.Prefix(), "users"),
				table.WithColumn("id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("username", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("mode", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("magic", ydb.Optional(ydb.TypeUint32)),
				table.WithColumn("score", ydb.Optional(ydb.TypeInt64)),
				table.WithColumn("updated", ydb.Optional(ydb.TypeTimestamp)),
				table.WithPrimaryKeyColumn("id"),
			)
		}),
	)
	if err != nil {
		return err
	}
	{
		const fill = `
			DECLARE $users AS List<Struct<
				id: Uint64?,
				username: Utf8?,
				mode: Uint64?,
				magic: Uint32?,
				score: Int64?,
				updated: Timestamp?>>;

			REPLACE INTO users
			SELECT
				id,
				username,
				mode,
				magic,
				score,
				updated
			FROM AS_TABLE($users);`

		users := Users{
			{ID: 0, Username: "Randy", Mode: 042},
			{ID: 1, Username: "Leo", Score: opt.OInt64(42)},
		}
		stmt, err := session.Prepare(ctx, withPragma(params.Prefix(), fill))
		if err != nil {
			return err
		}
		_, _, err = stmt.Execute(ctx, rwTX, table.NewQueryParameters(
			table.ValueParam("$users", users.ListValue()),
		))
		if err != nil {
			return err
		}
	}
	{
		const insert = `
			DECLARE $id AS Uint64?;
			DECLARE $username AS Utf8?;
			DECLARE $mode AS Uint64?;
			DECLARE $magic AS Uint32?;
			DECLARE $score AS Int64?;
			DECLARE $updated AS Timestamp?;

			UPSERT INTO users
				(id, username, mode, magic, score, updated)
			VALUES
				($id, $username, $mode, $magic, $score, $updated);`

		user := User{
			ID:       3,
			Username: "Elliot",
			Magic:    1,
			Score:    opt.OInt64(43),
			Updated:  time.Now(),
		}
		stmt, err := session.Prepare(ctx, withPragma(params.Prefix(), insert))
		if err != nil {
			return err
		}
		_, _, err = stmt.Execute(ctx, rwTX, user.QueryParameters())
		if err != nil {
			return err
		}
	}
	{
		const query = `
			SELECT * FROM users;`

		stmt, err := session.Prepare(ctx, withPragma(params.Prefix(), query))
		if err != nil {
			return err
		}
		_, res, err := stmt.Execute(ctx, roTX, nil)
		if err != nil {
			return err
		}

		res.NextSet()

		var users Users
		if err := (&users).Scan(res); err != nil {
			return err
		}
		for _, user := range users {
			log.Printf("> user %+v", user)
		}
	}
	{
		const query = `
			SELECT
				magic,
				AGGREGATE_LIST(username)
			FROM
				users
			GROUP BY
				magic;`

		stmt, err := session.Prepare(ctx, withPragma(params.Prefix(), query))
		if err != nil {
			return err
		}
		_, res, err := stmt.Execute(ctx, roTX, nil)
		if err != nil {
			return err
		}

		res.NextSet()

		var list MagicUsersList
		if err := (&list).Scan(res); err != nil {
			return err
		}
		for _, m := range list {
			log.Printf("> magic: %d", m.Magic)
			for _, user := range m.Users {
				log.Printf("  > user %+v", user)
			}
		}
	}

	return nil
}

func withPragma(prefix, query string) string {
	return `PRAGMA TablePathPrefix("` + prefix + `");` +
		query
}
