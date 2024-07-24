package ydb

//go:generate gtrace

import (
	"context"
	"strings"
)

type (
	//gtrace:gen
	//gtrace:set shortcut
	//gtrace:set context
	DriverTrace struct {
		OnDial func(DialStartInfo) func(DialDoneInfo)

		OnGetConn func(GetConnStartInfo) func(GetConnDoneInfo)

		OnPessimization func(PessimizationStartInfo) func(PessimizationDoneInfo)

		// Only for background.
		TrackConnStart func(TrackConnStartInfo)
		TrackConnDone  func(TrackConnDoneInfo)

		OnGetCredentials func(GetCredentialsStartInfo) func(GetCredentialsDoneInfo)

		OnDiscovery func(DiscoveryStartInfo) func(DiscoveryDoneInfo)

		OnOperation     func(OperationStartInfo) func(OperationDoneInfo)
		OnOperationWait func(OperationWaitInfo)

		OnStream func(StreamStartInfo) func(StreamDoneInfo)

		OnStreamRecv func(StreamRecvStartInfo) func(StreamRecvDoneInfo)
	}
)

// Method represents rpc method.
type Method string

// Name returns the rpc method name.
func (m Method) Name() (s string) {
	_, s = m.Split()
	return
}

// Service returns the rpc service name.
func (m Method) Service() (s string) {
	s, _ = m.Split()
	return
}

// Split returns service name and method.
func (m Method) Split() (service, method string) {
	i := strings.LastIndex(string(m), "/")
	if i == -1 {
		return string(m), string(m)
	}
	return strings.TrimPrefix(string(m[:i]), "/"), string(m[i+1:])
}

type (
	DialStartInfo struct {
		Context context.Context
		Address string
	}
	DialDoneInfo struct {
		Context context.Context
		Address string
		Error   error
	}
	GetConnStartInfo struct {
		Context context.Context
	}
	GetConnDoneInfo struct {
		Context context.Context
		Address string
		Error   error
	}
	PessimizationStartInfo struct {
		Context context.Context
		Address string
		Cause   error
	}
	PessimizationDoneInfo struct {
		Context context.Context
		Address string
		Error   error
	}
	TrackConnStartInfo struct {
		Address string
	}
	TrackConnDoneInfo struct {
		Address string
	}
	GetCredentialsStartInfo struct {
		Context context.Context
	}
	GetCredentialsDoneInfo struct {
		Context context.Context
		Token   bool
		Error   error
	}
	DiscoveryStartInfo struct {
		Context context.Context
		Address string
	}
	DiscoveryDoneInfo struct {
		Context   context.Context
		Endpoints []Endpoint
		Error     error
	}
	OperationStartInfo struct {
		Context context.Context
		Address string
		Method  Method
		Params  OperationParams
	}
	OperationWaitInfo struct {
		Context context.Context
		Address string
		Method  Method
		Params  OperationParams
		OpID    string
	}
	OperationDoneInfo struct {
		Context context.Context
		Address string
		Method  Method
		Params  OperationParams
		OpID    string
		Issues  IssueIterator
		Error   error
	}
	StreamStartInfo struct {
		Context context.Context
		Address string
		Method  Method
	}
	StreamRecvStartInfo struct {
		Context context.Context
		Address string
		Method  Method
	}
	StreamRecvDoneInfo struct {
		Context context.Context
		Address string
		Method  Method
		Issues  IssueIterator
		Error   error
	}
	StreamDoneInfo struct {
		Context context.Context
		Address string
		Method  Method
		Error   error
	}
)
