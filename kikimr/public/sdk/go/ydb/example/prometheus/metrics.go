package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/table"
	"github.com/prometheus/client_golang/prometheus"
)

func ifStr(cond bool, true, false string) string {
	if cond {
		return true
	}
	return false
}

type callScope struct {
	latency *prometheus.HistogramVec
	calls   *prometheus.GaugeVec
	value   *prometheus.GaugeVec
	errs    *prometheus.GaugeVec
}

func labelsToKeyValue(labels ...Label) map[string]string {
	kv := make(map[string]string, len(labels))
	for _, l := range labels {
		kv[l.Tag] = l.Value
	}
	return kv
}

func (s callScope) start(labels ...Label) *callTrace {
	s.calls.With(labelsToKeyValue(
		append([]Label{{
			Tag:   TagSuccess,
			Value: "wip",
		}}, labels...)...,
	)).Add(1)
	return &callTrace{
		start: time.Now(),
		scope: &s,
	}
}

type callTrace struct {
	start time.Time
	scope *callScope
}

func (t *callTrace) syncWithValue(error error, value float64, labels ...Label) {
	t.sync(error, labels...)
	if error == nil {
		t.scope.value.With(labelsToKeyValue(labels...)).Set(value)
	}
}

func (t *callTrace) syncWithSuccess(ok bool, labels ...Label) (callLabels []Label) {
	success := Label{
		Tag:   TagSuccess,
		Value: ifStr(ok, "true", "false"),
	}
	t.scope.calls.With(labelsToKeyValue(append([]Label{success}, labels...)...)).Add(1)
	t.scope.latency.With(labelsToKeyValue(append([]Label{success}, labels...)...)).Observe(float64(time.Since(t.start).Seconds()))
	return append([]Label{success}, labels...)
}

func (t *callTrace) sync(e error, labels ...Label) (callLabels []Label, errLabels []Label) {
	callLabels = t.syncWithSuccess(e == nil, labels...)
	if e != nil {
		errLabels = err(e, labels...)
		t.scope.errs.With(labelsToKeyValue(errLabels...)).Add(1)
	}
	return
}

type gaugeOpts struct {
	Namespace   string
	Subsystem   string
	Name        string
	Description string
}

func newGaugeOpts(opts prometheus.GaugeOpts) gaugeOpts {
	return gaugeOpts{
		Namespace:   opts.Namespace,
		Subsystem:   opts.Subsystem,
		Name:        opts.Name,
		Description: opts.Help,
	}
}

type histogramOpts struct {
	Namespace   string
	Subsystem   string
	Name        string
	Description string
}

func newHistogramOpts(opts prometheus.HistogramOpts) histogramOpts {
	return histogramOpts{
		Namespace:   opts.Namespace,
		Subsystem:   opts.Subsystem,
		Name:        opts.Name,
		Description: opts.Help,
	}
}

var (
	m          sync.Mutex
	gauges     = make(map[gaugeOpts]*prometheus.GaugeVec)
	histograms = make(map[histogramOpts]*prometheus.HistogramVec)
)

func gaugeVec(r prometheus.Registerer, namespace, name string, labelNames ...string) *prometheus.GaugeVec {
	opts := prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      name,
	}
	gaugeOpts := newGaugeOpts(opts)
	m.Lock()
	defer m.Unlock()
	if g, ok := gauges[gaugeOpts]; ok {
		return g
	}
	g := prometheus.NewGaugeVec(opts, labelNames)
	if err := r.Register(g); err != nil {
		panic(err)
	}
	gauges[gaugeOpts] = g
	return g
}

func histogramVec(r prometheus.Registerer, namespace, name string, labelNames ...string) *prometheus.HistogramVec {
	opts := prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      name,
		Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000},
	}
	histogramOpts := newHistogramOpts(opts)
	m.Lock()
	defer m.Unlock()
	if h, ok := histograms[histogramOpts]; ok {
		return h
	}
	h := prometheus.NewHistogramVec(opts, labelNames)
	if err := r.Register(h); err != nil {
		panic(err)
	}
	histograms[histogramOpts] = h
	return h
}

const (
	TagSource     = "source"
	TagName       = "name"
	TagMethod     = "method"
	TagError      = "error"
	TagErrCode    = "errCode"
	TagAddress    = "address"
	TagNodeID     = "nodeID"
	TagDataCenter = "destination"
	TagState      = "state"
	TagIdempotent = "idempotent"
	TagSuccess    = "success"
	TagStage      = "stage"
)

type Name string

type Label struct {
	Tag   string
	Value string
}

func err(err error, labels ...Label) []Label {
	var netErr *net.OpError
	var t *ydb.TransportError
	var o *ydb.OpError
	if errors.As(err, &netErr) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "network/" + netErr.Op + " -> " + netErr.Err.Error(),
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, io.EOF) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "io/EOF",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "context/DeadlineExceeded",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, context.Canceled) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "context/Canceled",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.As(err, &t) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "transport/" + t.Reason.String(),
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", int32(t.Reason)),
			},
		)
	}
	if errors.As(err, &o) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "operation/" + o.Reason.String(),
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", int32(o.Reason)),
			},
		)
	}
	return append(
		labels,
		Label{
			Tag:   TagError,
			Value: "unknown/" + strings.ReplaceAll(err.Error(), " ", "_"),
		},
		Label{
			Tag:   TagErrCode,
			Value: "-1",
		},
	)
}

func callGauges(r prometheus.Registerer, scope string, tags ...string) (s *callScope) {
	return &callScope{
		latency: histogramVec(r, scope, "latency", append([]string{TagSuccess}, tags...)...),
		calls:   gaugeVec(r, scope, "calls", append([]string{TagSuccess}, tags...)...),
		value:   gaugeVec(r, scope, "value", tags...),
		errs:    gaugeVec(r, scope, "errors", append([]string{TagError, TagErrCode}, tags...)...),
	}
}

func driverTrace(r prometheus.Registerer) ydb.DriverTrace {
	dial := callGauges(r, "driver_dial", TagAddress)
	get := callGauges(r, "driver_conn_get", TagAddress)
	pessimize := callGauges(r, "driver_conn_pessimize", TagAddress)
	discovery := callGauges(r, "driver_discovery", TagAddress)
	operation := callGauges(r, "driver_operation", TagAddress, TagMethod)
	stream := callGauges(r, "driver_stream_operation", TagAddress, TagMethod)
	return ydb.DriverTrace{
		OnDial: func(info ydb.DialStartInfo) func(ydb.DialDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			start := dial.start(address)
			return func(info ydb.DialDoneInfo) {
				start.sync(info.Error, address)
			}
		},
		OnGetConn: func(info ydb.GetConnStartInfo) func(ydb.GetConnDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: "wip",
			}
			start := get.start(address)
			return func(info ydb.GetConnDoneInfo) {
				start.sync(info.Error, Label{
					Tag:   TagAddress,
					Value: info.Address,
				})
			}
		},
		OnPessimization: func(info ydb.PessimizationStartInfo) func(ydb.PessimizationDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			start := pessimize.start(address)
			return func(info ydb.PessimizationDoneInfo) {
				start.sync(info.Error, Label{
					Tag:   TagAddress,
					Value: info.Address,
				})
			}
		},
		OnDiscovery: func(info ydb.DiscoveryStartInfo) func(ydb.DiscoveryDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			start := discovery.start(address)
			return func(info ydb.DiscoveryDoneInfo) {
				start.syncWithValue(info.Error, float64(len(info.Endpoints)), address)
			}
		},
		OnOperation: func(info ydb.OperationStartInfo) func(ydb.OperationDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			method := Label{
				Tag:   TagMethod,
				Value: string(info.Method),
			}
			start := operation.start(address, method)
			return func(info ydb.OperationDoneInfo) {
				start.sync(info.Error, Label{
					Tag:   TagAddress,
					Value: info.Address,
				}, Label{
					Tag:   TagMethod,
					Value: string(info.Method),
				})
			}
		},
		OnStream: func(info ydb.StreamStartInfo) func(ydb.StreamDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Address,
			}
			method := Label{
				Tag:   TagMethod,
				Value: string(info.Method),
			}
			start := stream.start(address, method)
			return func(info ydb.StreamDoneInfo) {
				start.sync(info.Error, Label{
					Tag:   TagAddress,
					Value: info.Address,
				}, Label{
					Tag:   TagMethod,
					Value: string(info.Method),
				})
			}
		},
		OnStreamRecv: nil,
	}
}

func nodeID(sessionID string) string {
	u, err := url.Parse(sessionID)
	if err != nil {
		panic(err)
	}
	return u.Query().Get("node_id")
}

func tableClientTrace(r prometheus.Registerer) table.ClientTrace {
	new := callGauges(r, "table_session_new", TagNodeID)
	delete := callGauges(r, "table_session_delete", TagNodeID)
	keepAlive := callGauges(r, "table_session_keep_alive", TagNodeID)
	prepare := callGauges(r, "table_session_prepare", TagNodeID)
	execute := callGauges(r, "table_session_execute", TagNodeID)
	streamRead := callGauges(r, "table_session_stream_read", TagNodeID)
	streamExecute := callGauges(r, "table_session_stream_execute", TagNodeID)
	txBegin := callGauges(r, "table_session_tx_begin", TagNodeID)
	txCommit := callGauges(r, "table_session_tx_commit", TagNodeID)
	txRollback := callGauges(r, "table_session_tx_rollback", TagNodeID)
	return table.ClientTrace{
		OnCreateSession: func(info table.CreateSessionStartInfo) func(table.CreateSessionDoneInfo) {
			start := new.start(Label{
				Tag:   TagNodeID,
				Value: "wip",
			})
			return func(info table.CreateSessionDoneInfo) {
				nodeID := Label{
					Tag: TagNodeID,
					Value: func() string {
						if info.Session != nil {
							return nodeID(info.Session.ID)
						}
						return ""
					}(),
				}
				lables, _ := start.sync(info.Error, nodeID)
				// publish empty delete call metric for register metrics on metrics storage
				delete.calls.With(labelsToKeyValue(lables...)).Add(0)
			}
		},
		OnKeepAlive: func(info table.KeepAliveStartInfo) func(table.KeepAliveDoneInfo) {
			nodeID := Label{
				Tag:   TagNodeID,
				Value: nodeID(info.Session.ID),
			}
			start := keepAlive.start(nodeID)
			return func(info table.KeepAliveDoneInfo) {
				start.sync(info.Error, nodeID)
			}
		},
		OnDeleteSession: func(info table.DeleteSessionStartInfo) func(table.DeleteSessionDoneInfo) {
			nodeID := Label{
				Tag:   TagNodeID,
				Value: nodeID(info.Session.ID),
			}
			start := delete.start(nodeID)
			return func(info table.DeleteSessionDoneInfo) {
				start.sync(info.Error, nodeID)
			}
		},
		OnPrepareDataQuery: func(info table.PrepareDataQueryStartInfo) func(table.PrepareDataQueryDoneInfo) {
			nodeID := Label{
				Tag:   TagNodeID,
				Value: nodeID(info.Session.ID),
			}
			start := prepare.start(nodeID)
			return func(info table.PrepareDataQueryDoneInfo) {
				start.sync(info.Error, nodeID)
			}
		},
		OnExecuteDataQuery: func(info table.ExecuteDataQueryStartInfo) func(table.ExecuteDataQueryDoneInfo) {
			nodeID := Label{
				Tag:   TagNodeID,
				Value: nodeID(info.Session.ID),
			}
			start := execute.start(nodeID)
			return func(info table.ExecuteDataQueryDoneInfo) {
				start.sync(info.Error, nodeID)
			}
		},
		OnStreamReadTable: func(info table.StreamReadTableStartInfo) func(table.StreamReadTableDoneInfo) {
			nodeID := Label{
				Tag:   TagNodeID,
				Value: nodeID(info.Session.ID),
			}
			start := streamRead.start(nodeID)
			return func(info table.StreamReadTableDoneInfo) {
				start.sync(info.Error, nodeID)
			}
		},
		OnStreamExecuteScanQuery: func(info table.StreamExecuteScanQueryStartInfo) func(table.StreamExecuteScanQueryDoneInfo) {
			nodeID := Label{
				Tag:   TagNodeID,
				Value: nodeID(info.Session.ID),
			}
			start := streamExecute.start(nodeID)
			return func(info table.StreamExecuteScanQueryDoneInfo) {
				start.sync(info.Error, nodeID)
			}
		},
		OnBeginTransaction: func(info table.BeginTransactionStartInfo) func(table.BeginTransactionDoneInfo) {
			nodeID := Label{
				Tag:   TagNodeID,
				Value: nodeID(info.Session.ID),
			}
			start := txBegin.start(nodeID)
			return func(info table.BeginTransactionDoneInfo) {
				start.sync(info.Error, nodeID)
			}
		},
		OnCommitTransaction: func(info table.CommitTransactionStartInfo) func(table.CommitTransactionDoneInfo) {
			nodeID := Label{
				Tag:   TagNodeID,
				Value: nodeID(info.Session.ID),
			}
			start := txCommit.start(nodeID)
			return func(info table.CommitTransactionDoneInfo) {
				start.sync(info.Error, nodeID)
			}
		},
		OnRollbackTransaction: func(info table.RollbackTransactionStartInfo) func(table.RollbackTransactionDoneInfo) {
			nodeID := Label{
				Tag:   TagNodeID,
				Value: nodeID(info.Session.ID),
			}
			start := txRollback.start(nodeID)
			return func(info table.RollbackTransactionDoneInfo) {
				start.sync(info.Error, nodeID)
			}
		},
	}
}

func tablePoolTrace(r prometheus.Registerer) table.SessionPoolTrace {
	return table.SessionPoolTrace{
		OnCreate:       nil,
		OnGet:          nil,
		OnWait:         nil,
		OnTake:         nil,
		OnTakeWait:     nil,
		OnPut:          nil,
		OnPutBusy:      nil,
		OnCloseSession: nil,
		OnClose:        nil,
	}
}
