package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"path"
	"text/template"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/example/internal/cli"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

type templateConfig struct {
	TablePathPrefix string
}

var fill = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

DECLARE $seriesData AS List<Struct<
	series_id: Uint64,
	title: Utf8,
	series_info: Utf8,
	release_date: Date,
	comment: Optional<Utf8>>>;

DECLARE $seasonsData AS List<Struct<
	series_id: Uint64,
	season_id: Uint64,
	title: Utf8,
	first_aired: Date,
	last_aired: Date>>;

DECLARE $episodesData AS List<Struct<
	series_id: Uint64,
	season_id: Uint64,
	episode_id: Uint64,
	title: Utf8,
	air_date: Date>>;

REPLACE INTO series
SELECT
	series_id,
	title,
	series_info,
	CAST(release_date AS Uint64) AS release_date,
	comment
FROM AS_TABLE($seriesData);

REPLACE INTO seasons
SELECT
	series_id,
	season_id,
	title,
	CAST(first_aired AS Uint64) AS first_aired,
	CAST(last_aired AS Uint64) AS last_aired
FROM AS_TABLE($seasonsData);

REPLACE INTO episodes
SELECT
	series_id,
	season_id,
	episode_id,
	title,
	CAST(air_date AS Uint64) AS air_date
FROM AS_TABLE($episodesData);
`))

type Command struct {
}

func (cmd *Command) ExportFlags(context.Context, *flag.FlagSet) {}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(
		connectCtx,
		params.ConnectParams,
		connect.WithSessionPoolIdleThreshold(time.Second*5),
		connect.WithSessionPoolKeepAliveMinSize(-1),
	)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	err = db.CleanupDatabase(ctx, params.Prefix(), "series", "episodes", "seasons")
	if err != nil {
		return err
	}

	err = db.EnsurePathExists(ctx, params.Prefix())
	if err != nil {
		return err
	}

	err = describeTableOptions(ctx, db.Table().Pool())
	if err != nil {
		return fmt.Errorf("describe table options error: %w", err)
	}

	err = createTables(ctx, db.Table().Pool(), params.Prefix())
	if err != nil {
		return fmt.Errorf("create tables error: %w", err)
	}

	err = describeTable(ctx, db.Table().Pool(), path.Join(
		params.Prefix(), "series",
	))
	if err != nil {
		return fmt.Errorf("describe table error: %w", err)
	}

	err = fillTablesWithData(ctx, db.Table().Pool(), params.Prefix())
	if err != nil {
		return fmt.Errorf("fill tables with data error: %w", err)
	}

	err = selectSimple(ctx, db.Table().Pool(), params.Prefix())
	if err != nil {
		return fmt.Errorf("select simple error: %w", err)
	}

	err = scanQuerySelect(ctx, db.Table().Pool(), params.Prefix())
	if err != nil {
		if !ydb.IsTransportError(err, ydb.TransportErrorUnimplemented) {
			return fmt.Errorf("scan query select error: %w", err)
		}
	}

	err = readTable(ctx, db.Table().Pool(), path.Join(
		params.Prefix(), "series",
	))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	return nil
}

func readTable(ctx context.Context, sp *table.SessionPool, path string) (err error) {
	var res *table.Result
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.StreamReadTable(ctx, path,
				table.ReadOrdered(),
				table.ReadColumn("series_id"),
				table.ReadColumn("title"),
				table.ReadColumn("release_date"),
			)
			return
		}),
	)
	if err != nil {
		return err
	}
	log.Printf("\n> read_table:")
	var (
		id    *uint64
		title *string
		date  *uint64
	)

	for res.NextResultSet(ctx, "series_id", "title", "release_date") {
		for res.NextRow() {
			err = res.Scan(&id, &title, &date)
			if err != nil {
				return err
			}
			log.Printf("#  %d %s %d", *id, *title, *date)
		}
	}
	if err := res.Err(); err != nil {
		return err
	}
	stats := res.Stats()
	for i := 0; ; i++ {
		phase, ok := stats.NextPhase()
		if !ok {
			break
		}
		log.Printf(
			"# phase #%d: took %s",
			i, phase.Duration,
		)
		for {
			tbl, ok := phase.NextTableAccess()
			if !ok {
				break
			}
			log.Printf(
				"#  accessed %s: read=(%drows, %dbytes)",
				tbl.Name, tbl.Reads.Rows, tbl.Reads.Bytes,
			)
		}
	}
	return nil
}

func describeTableOptions(ctx context.Context, sp *table.SessionPool) (err error) {
	var desc table.TableOptionsDescription
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			desc, err = s.DescribeTableOptions(ctx)
			return
		}),
	)
	if err != nil {
		return err
	}
	log.Println("\n> describe_table_options:")

	for i, p := range desc.TableProfilePresets {
		log.Printf("TableProfilePresets: %d/%d: %+v", i+1, len(desc.TableProfilePresets), p)
	}
	for i, p := range desc.StoragePolicyPresets {
		log.Printf("StoragePolicyPresets: %d/%d: %+v", i+1, len(desc.StoragePolicyPresets), p)
	}
	for i, p := range desc.CompactionPolicyPresets {
		log.Printf("CompactionPolicyPresets: %d/%d: %+v", i+1, len(desc.CompactionPolicyPresets), p)
	}
	for i, p := range desc.PartitioningPolicyPresets {
		log.Printf("PartitioningPolicyPresets: %d/%d: %+v", i+1, len(desc.PartitioningPolicyPresets), p)
	}
	for i, p := range desc.ExecutionPolicyPresets {
		log.Printf("ExecutionPolicyPresets: %d/%d: %+v", i+1, len(desc.ExecutionPolicyPresets), p)
	}
	for i, p := range desc.ReplicationPolicyPresets {
		log.Printf("ReplicationPolicyPresets: %d/%d: %+v", i+1, len(desc.ReplicationPolicyPresets), p)
	}
	for i, p := range desc.CachingPolicyPresets {
		log.Printf("CachingPolicyPresets: %d/%d: %+v", i+1, len(desc.CachingPolicyPresets), p)
	}

	return nil
}

func selectSimple(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			DECLARE $seriesID AS Uint64;
			$format = DateTime::Format("%Y-%m-%d");
			SELECT
				series_id,
				title,
				$format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
			FROM
				series
			WHERE
				series_id = $seriesID;
		`)),
		templateConfig{
			TablePathPrefix: prefix,
		},
	)
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)
	var res *table.Result
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, res, err = s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$seriesID", ydb.Uint64Value(1)),
				),
				table.WithQueryCachePolicy(
					table.WithQueryCachePolicyKeepInCache(),
				),
				table.WithCollectStatsModeBasic(),
			)
			return
		}),
	)
	if err != nil {
		return err
	}

	var (
		id    *uint64
		title *string
		date  *[]byte
	)

	for res.NextResultSet(ctx, "series_id", "title", "release_date") {
		for res.NextRow() {
			err = res.Scan(&id, &title, &date)
			if err != nil {
				return err
			}
			log.Printf(
				"\n> select_simple_transaction: %d %s %s",
				*id, *title, *date,
			)
		}
	}
	if err = res.Err(); err != nil {
		return err
	}
	return nil
}

func scanQuerySelect(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $series AS List<UInt64>;

			SELECT series_id, season_id, title, CAST(CAST(first_aired AS Date) AS String) AS first_aired
			FROM seasons
			WHERE series_id IN $series
		`)),
		templateConfig{
			TablePathPrefix: prefix,
		},
	)

	var res *table.Result
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.StreamExecuteScanQuery(ctx, query,
				table.NewQueryParameters(
					table.ValueParam("$series",
						ydb.ListValue(
							ydb.Uint64Value(1),
							ydb.Uint64Value(10),
						),
					),
				),
			)
			return
		}),
	)
	if err != nil {
		return err
	}
	var (
		seriesID uint64
		seasonID uint64
		title    string
		date     string // due to cast in select query
	)
	log.Print("\n> scan_query_select:")
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			err = res.ScanWithDefaults(&seriesID, &seasonID, &title, &date)
			if err != nil {
				return err
			}
			log.Printf("#  Season, SeriesId: %d, SeasonId: %d, Title: %s, Air date: %s", seriesID, seasonID, title, date)
		}
	}
	if err = res.Err(); err != nil {
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
				table.ValueParam("$seriesData", getSeriesData()),
				table.ValueParam("$seasonsData", getSeasonsData()),
				table.ValueParam("$episodesData", getEpisodesData()),
			))
			return err
		}),
	)
}

func createTables(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "series"),
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("series_info", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("release_date", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("comment", ydb.Optional(ydb.TypeUTF8)),
				table.WithPrimaryKeyColumn("series_id"),
			)
		}),
	)
	if err != nil {
		return err
	}

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "seasons"),
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("season_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("first_aired", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("last_aired", ydb.Optional(ydb.TypeUint64)),
				table.WithPrimaryKeyColumn("series_id", "season_id"),
			)
		}),
	)
	if err != nil {
		return err
	}

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "episodes"),
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("season_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("episode_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("air_date", ydb.Optional(ydb.TypeUint64)),
				table.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
			)
		}),
	)
	if err != nil {
		return err
	}

	return nil
}

func describeTable(ctx context.Context, sp *table.SessionPool, path string) (err error) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			desc, err := s.DescribeTable(ctx, path)
			if err != nil {
				return err
			}
			log.Printf("\n> describe table: %s", path)
			for _, c := range desc.Columns {
				log.Printf("column, name: %s, %s", c.Type, c.Name)
			}
			return nil
		}),
	)
	return
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
