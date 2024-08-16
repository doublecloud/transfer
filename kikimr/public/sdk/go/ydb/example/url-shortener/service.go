package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"net/http"
	"os"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/connect"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
)

const (
	invalidHashError = "'%s' is not a valid short path."
	hashNotFound     = "'%s' path is not found"
	invalidURLError  = "'%s' is not a valid URL."
)

var (
	indexPageContent = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>URL shortener</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">
    <script src="http://code.jquery.com/jquery-3.5.0.min.js" integrity="sha256-xNzN2a4ltkB44Mc/Jz3pT4iU1cmeR0FkXs4pru/JxaQ=" crossorigin="anonymous"></script>
    <style>
        body {
            margin-top: 30px;
        }
        .shorten {
            color: blue;
        }
    </style>
</head>
<body>

<div class="container">
    <div class="row-12">
        <form id="source-url-form">
            <div class="form-row">
                <div class="col-12">
                    <input id="source" type="text" class="form-control" placeholder="https://">
                </div>
            </div>
        </form>
    </div>
    <div class="row-12">
        <a id="shorten" class="shorten" href=""/>
    </div>
</div>

<script>
    $(function() {
        let $form = $("#source-url-form");
        let $source = $("#source");
        let $shorten = $("#shorten");

        $form.submit(function(e) {
            e.preventDefault();

            $.ajax({
                url: 'url?url=' + $source.val(),
                contentType: "application/text; charset=utf-8",
                traditional: true,
                success: function(data) {
                    $shorten.html(data);
					$shorten.attr('href', data);
                },
                error: function(data) {
                    $shorten.html('invalid url')
					$shorten.attr('href', '');
                }
            });
        });
    });
</script>

</body>
</html>`

	short = regexp.MustCompile(`[a-zA-Z0-9]`)
	long  = regexp.MustCompile(`https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+`)
)

func Hash(s string) (string, error) {
	hasher := fnv.New32a()
	_, err := hasher.Write([]byte(s))
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func isShortCorrect(link string) bool {
	return short.FindStringIndex(link) != nil
}

func isLongCorrect(link string) bool {
	return long.FindStringIndex(link) != nil
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

type templateConfig struct {
	TablePathPrefix string
}

type service struct {
	database string
	db       *connect.Connection
}

func NewService(ctx context.Context, connectParams connect.ConnectParams, connectTimeout time.Duration) (h *service, err error) {
	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	db, err := connect.New(connectCtx, connectParams)
	if err != nil {
		return nil, fmt.Errorf("connect error: %w", err)
	}
	h = &service{
		database: connectParams.Database(),
		db:       db,
	}
	err = h.createTable(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error on create table: %w", err)
	}
	return h, nil
}

func (s *service) Close() {
	s.db.Close()
}

func (s *service) createTable(ctx context.Context) (err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			CREATE TABLE urls (
				src Utf8,
				hash Utf8,

				PRIMARY KEY (hash)
			);
		`)),
		templateConfig{
			TablePathPrefix: s.database,
		},
	)
	return table.Retry(ctx, s.db.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			err := s.ExecuteSchemeQuery(ctx, query)
			return err
		}),
	)
}

func (s *service) insertShort(ctx context.Context, url string) (hash string, err error) {
	hash, err = Hash(url)
	if err != nil {
		return "", err
	}
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $hash as Utf8;
			DECLARE $src as Utf8;

			REPLACE INTO
				urls (hash, src)
			VALUES
				($hash, $src);
		`)),
		templateConfig{
			TablePathPrefix: s.database,
		},
	)
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	return hash, table.Retry(ctx, s.db.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				table.NewQueryParameters(
					table.ValueParam("$hash", ydb.UTF8Value(hash)),
					table.ValueParam("$src", ydb.UTF8Value(url)),
				),
				table.WithCollectStatsModeBasic(),
			)
			return
		}),
	)
}

func (s *service) selectLong(ctx context.Context, hash string) (url string, err error) {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $hash as Utf8;

			SELECT
				src
			FROM
				urls
			WHERE
				hash = $hash;
		`)),
		templateConfig{
			TablePathPrefix: s.database,
		},
	)
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)
	var res *table.Result
	err = table.Retry(ctx, s.db.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, res, err = s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$hash", ydb.UTF8Value(hash)),
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
		return "", err
	}
	defer func() {
		_ = res.Close()
	}()
	for res.NextSet() {
		for res.NextRow() {
			if res.SeekItem("src") {
				return res.OUTF8(), nil
			}
		}
	}
	return "", fmt.Errorf(hashNotFound, hash)
}

func writeResponse(w http.ResponseWriter, statusCode int, body string) {
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(body))
}

func (s *service) Router(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimLeft(r.URL.Path, "/")
	switch {
	case path == "":
		http.ServeContent(w, r, "", time.Now(), bytes.NewReader([]byte(indexPageContent)))
	case path == "url":
		url := r.URL.Query().Get("url")
		if !isLongCorrect(url) {
			writeResponse(w, http.StatusBadRequest, fmt.Sprintf(invalidURLError, url))
			return
		}
		hash, err := s.insertShort(r.Context(), url)
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/text")
		protocol := "http://"
		if r.TLS == nil {
			protocol = "http://"
		}
		writeResponse(w, http.StatusOK, protocol+r.Host+"/"+hash)
	default:
		if !isShortCorrect(path) {
			writeResponse(w, http.StatusBadRequest, fmt.Sprintf(invalidHashError, path))
			return
		}
		url, err := s.selectLong(r.Context(), path)
		if err != nil {
			writeResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		http.Redirect(w, r, url, http.StatusSeeOther)
	}
}

// Serverless is an entrypoint for serverless yandex function
func Serverless(w http.ResponseWriter, r *http.Request) {
	service, err := NewService(r.Context(), connect.MustConnectionString(os.Getenv("YDB")), time.Second)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	service.Router(w, r)
}
