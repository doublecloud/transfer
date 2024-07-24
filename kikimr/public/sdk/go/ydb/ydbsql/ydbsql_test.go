package ydbsql

import (
	"database/sql"
	"net/url"
	"os"
	"testing"
)

func TestLegacyDriverValidateDataSourceURI(t *testing.T) {
	for _, test := range []struct {
		name string
		url  string
		err  bool
	}{
		{
			name: "welformed",
			url:  "ydb://endpoint/database?auth-token=xxx",
			err:  false,
		},
		{
			name: "no token",
			url:  "ydb://endpoint/database",
			err:  true,
		},
		{
			name: "no endpoint",
			url:  "ydb:///database?auth-token=xxx",
			err:  true,
		},
		{
			name: "no database",
			url:  "ydb://endpoint?auth-token=xxx",
			err:  true,
		},
		{
			name: "bad scheme",
			url:  "http://endpoint/database?auth-token=xxx",
			err:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			u, err := url.Parse(test.url)
			if err != nil {
				t.Fatal(err)
			}
			err = validateURL(u)
			if !test.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if test.err && err == nil {
				t.Fatalf("unexpected nil error")
			}
		})
	}
}

func TestLegacyDriverOpen(t *testing.T) {
	if _, ok := os.LookupEnv("YDB_CONNECTION_STRING"); !ok {
		t.Skip("need to be tested with docker")
	}

	db, err := sql.Open("ydb/v2",
		os.Getenv("YDB_CONNECTION_STRING")+"&"+urlAuthToken+"="+os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}
