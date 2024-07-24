package table

//go:generate gtrace

import (
	"context"
	"time"
)

// ClientTrace contains options for tracing table client activity.
type (
	//gtrace:gen
	//gtrace:set shortcut
	//gtrace:set context
	ClientTrace struct {
		OnCreateSession func(CreateSessionStartInfo) func(CreateSessionDoneInfo)

		OnKeepAlive func(KeepAliveStartInfo) func(KeepAliveDoneInfo)

		OnDeleteSession func(DeleteSessionStartInfo) func(DeleteSessionDoneInfo)

		OnPrepareDataQuery func(PrepareDataQueryStartInfo) func(PrepareDataQueryDoneInfo)

		OnExecuteDataQuery func(ExecuteDataQueryStartInfo) func(ExecuteDataQueryDoneInfo)

		OnStreamReadTable func(StreamReadTableStartInfo) func(StreamReadTableDoneInfo)

		OnStreamExecuteScanQuery func(StreamExecuteScanQueryStartInfo) func(StreamExecuteScanQueryDoneInfo)

		OnBeginTransaction func(BeginTransactionStartInfo) func(BeginTransactionDoneInfo)

		OnCommitTransaction func(CommitTransactionStartInfo) func(CommitTransactionDoneInfo)

		OnRollbackTransaction func(RollbackTransactionStartInfo) func(RollbackTransactionDoneInfo)
	}
)

// RetryTrace contains options for tracing retry client activity.
type (
	//gtrace:gen
	//gtrace:set shortcut
	//gtrace:set context
	RetryTrace struct {
		OnLoop func(RetryLoopStartInfo) func(RetryLoopDoneInfo)
	}
)

type (
	CreateSessionStartInfo struct {
		Context context.Context
	}
	CreateSessionDoneInfo struct {
		Context  context.Context
		Session  *Session
		Endpoint string
		Latency  time.Duration
		Error    error
	}
	KeepAliveStartInfo struct {
		Context context.Context
		Session *Session
	}
	KeepAliveDoneInfo struct {
		Context     context.Context
		Session     *Session
		SessionInfo SessionInfo
		Error       error
	}
	DeleteSessionStartInfo struct {
		Context context.Context
		Session *Session
	}
	DeleteSessionDoneInfo struct {
		Context context.Context
		Session *Session
		Latency time.Duration
		Error   error
	}
	PrepareDataQueryStartInfo struct {
		Context context.Context
		Session *Session
		Query   string
	}
	PrepareDataQueryDoneInfo struct {
		Context context.Context
		Session *Session
		Query   string
		Result  *DataQuery
		Cached  bool
		Error   error
	}
	ExecuteDataQueryStartInfo struct {
		Context    context.Context
		Session    *Session
		TxID       string
		Query      *DataQuery
		Parameters *QueryParameters
	}
	ExecuteDataQueryDoneInfo struct {
		Context    context.Context
		Session    *Session
		TxID       string
		Query      *DataQuery
		Parameters *QueryParameters
		Prepared   bool
		Result     *Result
		Error      error
	}
	StreamReadTableStartInfo struct {
		Context context.Context
		Session *Session
	}
	StreamReadTableDoneInfo struct {
		Context context.Context
		Session *Session
		Result  *Result
		Error   error
	}
	StreamExecuteScanQueryStartInfo struct {
		Context    context.Context
		Session    *Session
		Query      *DataQuery
		Parameters *QueryParameters
	}
	StreamExecuteScanQueryDoneInfo struct {
		Context    context.Context
		Session    *Session
		Query      *DataQuery
		Parameters *QueryParameters
		Result     *Result
		Error      error
	}
	BeginTransactionStartInfo struct {
		Context context.Context
		Session *Session
	}
	BeginTransactionDoneInfo struct {
		Context context.Context
		Session *Session
		TxID    string
		Error   error
	}
	CommitTransactionStartInfo struct {
		Context context.Context
		Session *Session
		TxID    string
	}
	CommitTransactionDoneInfo struct {
		Context context.Context
		Session *Session
		TxID    string
		Error   error
	}
	RollbackTransactionStartInfo struct {
		Context context.Context
		Session *Session
		TxID    string
	}
	RollbackTransactionDoneInfo struct {
		Context context.Context
		Session *Session
		TxID    string
		Error   error
	}
	RetryLoopStartInfo struct {
		Context context.Context
	}
	RetryLoopDoneInfo struct {
		Context  context.Context
		Latency  time.Duration
		Attempts int
	}
)

// SessionPoolTrace contains options for tracing session pool activity.
type (
	//gtrace:gen
	//gtrace:set shortcut
	//gtrace:set context
	SessionPoolTrace struct {
		OnCreate       func(SessionPoolCreateStartInfo) func(SessionPoolCreateDoneInfo)
		OnGet          func(SessionPoolGetStartInfo) func(SessionPoolGetDoneInfo)
		OnWait         func(SessionPoolWaitStartInfo) func(SessionPoolWaitDoneInfo)
		OnBusyCheck    func(SessionPoolBusyCheckStartInfo) func(SessionPoolBusyCheckDoneInfo)
		OnTake         func(SessionPoolTakeStartInfo) func(SessionPoolTakeDoneInfo)
		OnTakeWait     func(SessionPoolTakeWaitInfo)
		OnPut          func(SessionPoolPutStartInfo) func(SessionPoolPutDoneInfo)
		OnPutBusy      func(SessionPoolPutBusyStartInfo) func(SessionPoolPutBusyDoneInfo)
		OnCloseSession func(SessionPoolCloseSessionStartInfo) func(SessionPoolCloseSessionDoneInfo)
		OnClose        func(SessionPoolCloseStartInfo) func(SessionPoolCloseDoneInfo)
	}
)

type (
	SessionPoolCreateStartInfo struct {
		Context context.Context
	}
	SessionPoolCreateDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	SessionPoolGetStartInfo struct {
		Context context.Context
	}
	SessionPoolGetDoneInfo struct {
		Context       context.Context
		Session       *Session
		Latency       time.Duration
		RetryAttempts int
		Error         error
	}
	SessionPoolWaitStartInfo struct {
		Context context.Context
	}
	SessionPoolWaitDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	SessionPoolBusyCheckStartInfo struct {
		Context context.Context
		Session *Session
	}
	SessionPoolBusyCheckDoneInfo struct {
		Context context.Context
		Session *Session
		Reused  bool
		Error   error
	}
	SessionPoolTakeStartInfo struct {
		Context context.Context
		Session *Session
	}
	SessionPoolTakeWaitInfo struct {
		Context context.Context
		Session *Session
	}
	SessionPoolTakeDoneInfo struct {
		Context context.Context
		Session *Session
		Took    bool
		Error   error
	}
	SessionPoolPutStartInfo struct {
		Context context.Context
		Session *Session
	}
	SessionPoolPutDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	SessionPoolPutBusyStartInfo struct {
		Context context.Context
		Session *Session
	}
	SessionPoolPutBusyDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	SessionPoolCloseSessionStartInfo struct {
		Context context.Context
		Session *Session
	}
	SessionPoolCloseSessionDoneInfo struct {
		Context context.Context
		Session *Session
		Error   error
	}
	SessionPoolCloseStartInfo struct {
		Context context.Context
	}
	SessionPoolCloseDoneInfo struct {
		Context context.Context
		Error   error
	}
)

type (
	//gtrace:gen
	//gtrace:set shortcut
	createSessionTrace struct {
		OnCheckEnoughSpace            func(enoughSpace bool)
		OnCreateSessionGoroutineStart func() func(r createSessionResult)
		OnStartSelect                 func()
		OnReadResult                  func(r createSessionResult)
		OnContextDone                 func()
		OnPutSession                  func(session *Session, err error)
	}

	createSessionTraceContextKey struct{}
)

func withCreateSessionTrace(ctx context.Context, trace createSessionTrace) context.Context {
	return context.WithValue(ctx,
		createSessionTraceContextKey{},
		contextCreateSessionTrace(ctx).Compose(trace),
	)
}

func contextCreateSessionTrace(ctx context.Context) createSessionTrace {
	t, _ := ctx.Value(createSessionTraceContextKey{}).(createSessionTrace)
	return t
}

type createSessionResult struct {
	s   *Session
	err error
}
