package ydbsql

//go:generate gtrace

import "context"

type (
	//gtrace:gen
	//gtrace:set shortcut
	//gtrace:set context
	Trace struct {
		OnDial func(DialStartInfo) func(DialDoneInfo)
	}
)

type (
	DialStartInfo struct {
		Context context.Context
	}
	DialDoneInfo struct {
		Context context.Context
		Error   error
	}
)
