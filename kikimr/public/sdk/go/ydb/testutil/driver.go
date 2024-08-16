package testutil

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

var ErrNotImplemented = errors.New("testutil: not implemented")

type MethodCode uint

func (m MethodCode) String() string {
	return codeToString[m]
}

const (
	UnknownMethod MethodCode = iota
	TableCreateSession
	TableDeleteSession
	TableKeepAlive
	TableCreateTable
	TableDropTable
	TableAlterTable
	TableCopyTable
	TableDescribeTable
	TableExplainDataQuery
	TablePrepareDataQuery
	TableExecuteDataQuery
	TableExecuteSchemeQuery
	TableBeginTransaction
	TableCommitTransaction
	TableRollbackTransaction
	TableDescribeTableOptions
	TableStreamReadTable
	TableStreamExecuteScanQuery
)

var grpcMethodToCode = map[string]MethodCode{
	"/Ydb.Table.V1.TableService/CreateSession":          TableCreateSession,
	"/Ydb.Table.V1.TableService/DeleteSession":          TableDeleteSession,
	"/Ydb.Table.V1.TableService/KeepAlive":              TableKeepAlive,
	"/Ydb.Table.V1.TableService/CreateTable":            TableCreateTable,
	"/Ydb.Table.V1.TableService/DropTable":              TableDropTable,
	"/Ydb.Table.V1.TableService/AlterTable":             TableAlterTable,
	"/Ydb.Table.V1.TableService/CopyTable":              TableCopyTable,
	"/Ydb.Table.V1.TableService/DescribeTable":          TableDescribeTable,
	"/Ydb.Table.V1.TableService/ExplainDataQuery":       TableExplainDataQuery,
	"/Ydb.Table.V1.TableService/PrepareDataQuery":       TablePrepareDataQuery,
	"/Ydb.Table.V1.TableService/ExecuteDataQuery":       TableExecuteDataQuery,
	"/Ydb.Table.V1.TableService/ExecuteSchemeQuery":     TableExecuteSchemeQuery,
	"/Ydb.Table.V1.TableService/BeginTransaction":       TableBeginTransaction,
	"/Ydb.Table.V1.TableService/CommitTransaction":      TableCommitTransaction,
	"/Ydb.Table.V1.TableService/RollbackTransaction":    TableRollbackTransaction,
	"/Ydb.Table.V1.TableService/DescribeTableOptions":   TableDescribeTableOptions,
	"/Ydb.Table.V1.TableService/StreamReadTable":        TableStreamReadTable,
	"/Ydb.Table.V1.TableService/StreamExecuteScanQuery": TableStreamExecuteScanQuery,
}

var codeToString = map[MethodCode]string{
	TableCreateSession:          lastSegment("/Ydb.Table.V1.TableService/CreateSession"),
	TableDeleteSession:          lastSegment("/Ydb.Table.V1.TableService/DeleteSession"),
	TableKeepAlive:              lastSegment("/Ydb.Table.V1.TableService/KeepAlive"),
	TableCreateTable:            lastSegment("/Ydb.Table.V1.TableService/CreateTable"),
	TableDropTable:              lastSegment("/Ydb.Table.V1.TableService/DropTable"),
	TableAlterTable:             lastSegment("/Ydb.Table.V1.TableService/AlterTable"),
	TableCopyTable:              lastSegment("/Ydb.Table.V1.TableService/CopyTable"),
	TableDescribeTable:          lastSegment("/Ydb.Table.V1.TableService/DescribeTable"),
	TableExplainDataQuery:       lastSegment("/Ydb.Table.V1.TableService/ExplainDataQuery"),
	TablePrepareDataQuery:       lastSegment("/Ydb.Table.V1.TableService/PrepareDataQuery"),
	TableExecuteDataQuery:       lastSegment("/Ydb.Table.V1.TableService/ExecuteDataQuery"),
	TableExecuteSchemeQuery:     lastSegment("/Ydb.Table.V1.TableService/ExecuteSchemeQuery"),
	TableBeginTransaction:       lastSegment("/Ydb.Table.V1.TableService/BeginTransaction"),
	TableCommitTransaction:      lastSegment("/Ydb.Table.V1.TableService/CommitTransaction"),
	TableRollbackTransaction:    lastSegment("/Ydb.Table.V1.TableService/RollbackTransaction"),
	TableDescribeTableOptions:   lastSegment("/Ydb.Table.V1.TableService/DescribeTableOptions"),
	TableStreamReadTable:        lastSegment("/Ydb.Table.V1.TableService/StreamReadTable"),
	TableStreamExecuteScanQuery: lastSegment("/Ydb.Table.V1.TableService/StreamExecuteScanQuery"),
}

func setField(name string, dst, value interface{}) {
	x := reflect.ValueOf(dst).Elem()
	t := x.Type()
	f, ok := t.FieldByName(name)
	if !ok {
		panic(fmt.Sprintf(
			"ydb/testutil: struct %s has no field %q",
			t, name,
		))
	}
	v := reflect.ValueOf(value)
	if f.Type.Kind() != v.Type().Kind() {
		panic(fmt.Sprintf(
			"ydb/testutil: struct %s field %q is type of %s, not %s",
			t, name, f.Type, v.Type(),
		))
	}
	x.FieldByName(f.Name).Set(v)
}

func getField(name string, src, dst interface{}) bool {
	var fn func(x reflect.Value, seg ...string) bool
	fn = func(x reflect.Value, seg ...string) bool {
		if x.Kind() == reflect.Ptr {
			x = x.Elem()
		}
		t := x.Type()
		f, ok := t.FieldByName(seg[0])
		if !ok {
			return false
		}
		fv := x.FieldByName(seg[0])
		if fv.Kind() == reflect.Ptr && fv.IsNil() {
			return false
		}
		if len(seg) > 1 {
			return fn(fv.Elem(), seg[1:]...)
		}

		v := reflect.ValueOf(dst)
		if v.Type().Kind() != reflect.Ptr {
			panic("ydb/testutil: destination value must be a pointer")
		}
		if v.Type().Elem().Kind() != fv.Type().Kind() {
			panic(fmt.Sprintf(
				"ydb/testutil: struct %s field %q is type of %s, not %s",
				t, name, f.Type, v.Type(),
			))
		}

		v.Elem().Set(fv)

		return true
	}
	return fn(reflect.ValueOf(src).Elem(), strings.Split(name, ".")...)
}

type TableCreateSessionResult struct {
	R interface{}
}

func (t TableCreateSessionResult) SetSessionID(id string) {
	setField("SessionId", t.R, id)
}

type TableKeepAliveResult struct {
	R interface{}
}

func (t TableKeepAliveResult) SetSessionStatus(ready bool) {
	var status Ydb_Table.KeepAliveResult_SessionStatus
	if ready {
		status = Ydb_Table.KeepAliveResult_SESSION_STATUS_READY
	} else {
		status = Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY
	}
	setField("SessionStatus", t.R, status)
}

type TableBeginTransactionResult struct {
	R interface{}
}

func (t TableBeginTransactionResult) SetTransactionID(id string) {
	setField("TxMeta", t.R, &Ydb_Table.TransactionMeta{
		Id: id,
	})
}

type TableExecuteDataQueryResult struct {
	R interface{}
}

func (t TableExecuteDataQueryResult) SetTransactionID(id string) {
	setField("TxMeta", t.R, &Ydb_Table.TransactionMeta{
		Id: id,
	})
}

type TableExecuteDataQueryRequest struct {
	R interface{}
}

func (t TableExecuteDataQueryRequest) SessionID() (id string) {
	getField("SessionId", t.R, &id)
	return
}

func (t TableExecuteDataQueryRequest) TransactionID() (id string, ok bool) {
	ok = getField("TxControl.TxSelector.TxId", t.R, &id)
	return
}

func (t TableExecuteDataQueryRequest) KeepInCache() (keepInCache bool, ok bool) {
	ok = getField("QueryCachePolicy.KeepInCache", t.R, &keepInCache)
	return
}

type TablePrepareDataQueryResult struct {
	R interface{}
}

func (t TablePrepareDataQueryResult) SetQueryID(id string) {
	setField("QueryId", t.R, id)
}

type Driver struct {
	OnCall       func(ctx context.Context, code MethodCode, req, res interface{}) error
	OnStreamRead func(ctx context.Context, code MethodCode, req, res interface{}, process func(error)) error
	OnClose      func() error
}

func (d *Driver) Call(ctx context.Context, op ydb.Operation) (ydb.CallInfo, error) {
	if d.OnCall == nil {
		return nil, ErrNotImplemented
	}
	method, req, res, _ := internal.Unwrap(op)
	code := grpcMethodToCode[method]

	// NOTE: req and res may be converted to testutil inner structs, which are
	// mirrors of grpc api envelopes.
	return nil, d.OnCall(ctx, code, req, res)
}

func (d *Driver) StreamRead(ctx context.Context, op ydb.StreamOperation) (ydb.CallInfo, error) {
	if d.OnStreamRead == nil {
		return nil, ErrNotImplemented
	}
	method, req, res, processor := internal.UnwrapStreamOperation(op)
	code := grpcMethodToCode[method]

	return nil, d.OnStreamRead(ctx, code, req, res, processor)
}

func (d *Driver) Close() error {
	if d.OnClose == nil {
		return ErrNotImplemented
	}
	return d.OnClose()
}

func lastSegment(m string) string {
	s := strings.Split(m, "/")
	return s[len(s)-1]
}
