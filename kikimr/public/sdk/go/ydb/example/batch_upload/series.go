package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"net/url"
	"os"
	"path"
	"syscall"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/example/internal/cli"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/table"
)

type Command struct {
	rps   int
	infly int
	count int
}

func (cmd *Command) ExportFlags(_ context.Context, flagSet *flag.FlagSet) {
	flagSet.IntVar(&cmd.rps, "rps", 100, "limit write rate")
	flagSet.IntVar(&cmd.infly, "infly", 10, "limit infly requests")
	flagSet.IntVar(&cmd.count, "count", 1000, "count requests")
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	connectCtx, cancel := context.WithTimeout(ctx, params.ConnectTimeout)
	defer cancel()
	db, err := connect.New(connectCtx, params.ConnectParams)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer db.Close()

	name := path.Join(params.Prefix(), "upload_example")

	err = db.CleanupDatabase(ctx, params.Prefix(), "upload_example")
	if err != nil {
		return err
	}
	err = db.EnsurePathExists(ctx, params.Prefix())
	if err != nil {
		return err
	}

	err = createTable(ctx, db.Table().Pool(), name)
	if err != nil {
		return fmt.Errorf("create tables error: %w", err)
	}

	// make input generator of count
	query := fmt.Sprintf(`
		DECLARE $items AS
			'List<Struct<
				host_uid: Uint64?,
				url_uid: Uint64?,
				url: Utf8?,
				page: Utf8?>>';

		REPLACE INTO [%v]
			SELECT * FROM AS_TABLE($items);`, name)
	packSize := 11
	t := initTracker(cmd.count, cmd.infly)
	jobs := make(chan ItemList)
	for i := 0; i < cmd.infly; i++ {
		go uploadWorker(ctx, db.Table().Pool(), cmd.rps, query, jobs, t.track)
	}

	fmt.Printf(`Uploading...
  Do 'kill -USR1 %v' for progress datails
  Do 'kill -SIGINT %v' to cancel
`, os.Getpid(), os.Getpid())
	t.respondSignal(syscall.SIGUSR1)

	jobsCount := 0
loop:
	for i := 0; i < cmd.count; {
		pack := ItemList{}
		for ; i < cmd.count && len(pack) < packSize; i++ {
			item, err := generateItem(i)
			if err != nil {
				return err
			}
			pack = append(pack, *item)
		}

		select {
		case <-ctx.Done():
			break loop
		case jobs <- pack:
			jobsCount++
		}
	}
	close(jobs)

	select {
	case <-ctx.Done():
		close(t.stop)
		err = <-t.done
	case err = <-t.done:
	}

	t.report()

	return err
}

func uploadWorker(ctx context.Context, sp *table.SessionPool, rps int, query string, jobs <-chan ItemList,
	res chan<- result) {

	throttle := time.Tick(time.Second / time.Duration(rps))

	for j := range jobs {
		<-throttle

		writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())
		err := table.Retry(ctx, sp,
			table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
				stmt, err := s.Prepare(ctx, query)
				if err != nil {
					return err
				}
				_, _, err = stmt.Execute(ctx, writeTx,
					table.NewQueryParameters(
						table.ValueParam("$items", j.ListValue()),
					))
				return err
			}))
		if err != nil {
			res <- result{err, 0, len(j)}
		} else {
			res <- result{err, len(j), 0}
		}
	}
}

func createTable(ctx context.Context, sp *table.SessionPool, path string) (err error) {
	fmt.Printf(" create table %v\n", path)

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path,
				table.WithColumn("host_uid", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("url_uid", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("url", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("page", ydb.Optional(ydb.TypeUTF8)),
				table.WithPrimaryKeyColumn("host_uid", "url_uid"))
		}))
	if err != nil {
		return err
	}

	return nil
}

func generateItem(i int) (*Item, error) {
	urlNo := i
	hostNo := urlNo / 10

	rawURL := fmt.Sprintf("http://host-%v.ru:80/path_with_id_%v", hostNo, urlNo)
	host, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	urlHash := hash(rawURL)
	hostHash := hash(host.Scheme + host.Host)

	page := fmt.Sprintf("the page were page_num='%v'URL='%v' URLUID='%v' HostUID='%v'",
		urlNo, rawURL, urlHash, hostHash)

	return &Item{
		HostUID: hostHash,
		URLUID:  urlHash,
		URL:     rawURL,
		Page:    page,
	}, nil
}

func hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
