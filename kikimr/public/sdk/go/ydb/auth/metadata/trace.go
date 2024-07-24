package metadata

//go:generate gtrace

import (
	"net"
	"time"
)

type (
	//gtrace:gen
	//gtrace:set shortcut
	ClientTrace struct {
		OnDial         func(DialStartInfo) func(DialDoneInfo)
		OnWriteRequest func(WriteRequestStartInfo) func(WriteRequestDoneInfo)
		OnReadResponse func(ReadResponseStartInfo) func(ReadResponseDoneInfo)
	}
)

type (
	DialStartInfo struct {
		Network string
		Addr    string
	}
	DialDoneInfo struct {
		Network string
		Addr    string
		Conn    net.Conn
		Error   error
	}
	WriteRequestStartInfo struct {
		Conn net.Conn
	}
	WriteRequestDoneInfo struct {
		Conn  net.Conn
		Error error
	}
	ReadResponseStartInfo struct {
		Conn net.Conn
	}
	ReadResponseDoneInfo struct {
		Conn    net.Conn
		Code    string
		Expires time.Time
		Error   error
	}
)
