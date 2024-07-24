package main

import "time"

//go:generate ydbgen -seek position

//ydb:gen value
type User struct {
	ID   uint64 `ydb:"column:user_id"`
	Name string
	Age  uint32
}

//ydb:gen value
type UsersList []User

//ydb:gen value,scan
type Series struct {
	ID             uint64 `ydb:"column:series_id"`
	Title          string
	Info           string
	ReleaseDate    time.Time `ydb:"type:datetime?"`
	Views          uint64
	UploadedUserID uint64
}

//ydb:gen value,scan
type SeriesList []Series
