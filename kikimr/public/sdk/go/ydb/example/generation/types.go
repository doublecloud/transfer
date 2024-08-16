package main

import (
	_ "strconv"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/opt"
)

//go:generate ydbgen

// User represents a user of application.
//
//ydb:gen
type User struct {
	ID       uint64
	Username string
	Mode     uint8     `ydb:"type:uint64?,conv:assert"`
	Magic    uint      `ydb:"type:uint32?,conv:unsafe"`
	Score    opt.Int64 `ydb:"type:int64?"`
	Updated  time.Time `ydb:"type:timestamp?"`
	Data     []byte    `ydb:"-"`
}

//ydb:gen scan,value
type Users []User

//ydb:gen scan
//ydb:set seek:position
type MagicUsers struct {
	Magic uint     `ydb:"type:uint32?,conv:unsafe"`
	Users []string `ydb:"type:list<utf8>"`
}

//ydb:gen scan
type MagicUsersList []MagicUsers
