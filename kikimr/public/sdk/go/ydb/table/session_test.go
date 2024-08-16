package table

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal/cache/lru"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/testutil"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

func TestSessionKeepAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		status Ydb_Table.KeepAliveResult_SessionStatus
		e      error
	)
	b := StubBuilder{
		T: t,
		Handler: methodHandlers{
			testutil.TableKeepAlive: func(req, res interface{}) error {
				r, _ := res.(*Ydb_Table.KeepAliveResult)
				r.SessionStatus = status

				return e
			},
		},
	}
	s, err := b.CreateSession(ctx)
	if err != nil {
		t.Fatal(err)
	}

	{
		e = errors.New("any error")
		_, err := s.KeepAlive(ctx)
		if err == nil {
			t.Fatal(err)
		}
	}
	{
		status, e = Ydb_Table.KeepAliveResult_SESSION_STATUS_READY, nil
		info, err := s.KeepAlive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != SessionReady {
			t.Fatalf("Result %v differ from, expectd %v", info.Status, SessionReady)
		}
	}
	{
		status, e = Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY, nil
		info, err := s.KeepAlive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != SessionBusy {
			t.Fatalf("Result %v differ from, expectd %v", info.Status, SessionBusy)
		}
	}
}

func TestSessionDescribeTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		result *Ydb_Table.DescribeTableResult
		e      error
	)
	b := StubBuilder{
		T: t,
		Handler: methodHandlers{
			testutil.TableDescribeTable: func(req, res interface{}) error {
				r, _ := res.(*Ydb_Table.DescribeTableResult)
				r.Reset()
				proto.Merge(r, result)
				return e
			},
		},
	}
	s, err := b.CreateSession(ctx)
	if err != nil {
		t.Fatal(err)
	}

	{
		e = errors.New("any error")
		_, err = s.DescribeTable(ctx, "")
		if err == nil {
			t.Fatal(err)
		}
	}
	{
		e = nil
		expect := Description{
			Name:       "testName",
			PrimaryKey: []string{"testKey"},
			Columns: []Column{
				{
					Name:   "testColumn",
					Type:   ydb.Void(),
					Family: "testFamily",
				},
			},
			KeyRanges: []KeyRange{
				{
					From: nil,
					To:   ydb.Int64Value(100500),
				},
				{
					From: ydb.Int64Value(100500),
					To:   nil,
				},
			},
			ColumnFamilies: []ColumnFamily{
				{
					Name:         "testFamily",
					Data:         StoragePool{},
					Compression:  ColumnFamilyCompressionLZ4,
					KeepInMemory: ydb.FeatureEnabled,
				},
			},
			Attributes: map[string]string{},
			ReadReplicaSettings: ReadReplicasSettings{
				Type:  ReadReplicasAnyAzReadReplicas,
				Count: 42,
			},
			StorageSettings: StorageSettings{
				TableCommitLog0:    StoragePool{Media: "m1"},
				TableCommitLog1:    StoragePool{Media: "m2"},
				External:           StoragePool{Media: "m3"},
				StoreExternalBlobs: ydb.FeatureEnabled,
			},
			Indexes: []IndexDescription{},
		}
		result = &Ydb_Table.DescribeTableResult{
			Self: &Ydb_Scheme.Entry{
				Name:                 expect.Name,
				Owner:                "",
				Type:                 0,
				EffectivePermissions: nil,
				Permissions:          nil,
			},
			Columns: []*Ydb_Table.ColumnMeta{
				{
					Name:   expect.Columns[0].Name,
					Type:   internal.TypeToYDB(expect.Columns[0].Type),
					Family: "testFamily",
				},
			},
			PrimaryKey: expect.PrimaryKey,
			ShardKeyBounds: []*Ydb.TypedValue{
				internal.ValueToYDB(expect.KeyRanges[0].To),
			},
			Indexes:    nil,
			TableStats: nil,
			ColumnFamilies: []*Ydb_Table.ColumnFamily{
				{
					Name:         "testFamily",
					Data:         nil,
					Compression:  Ydb_Table.ColumnFamily_COMPRESSION_LZ4,
					KeepInMemory: Ydb.FeatureFlag_ENABLED,
				},
			},
			ReadReplicasSettings: expect.ReadReplicaSettings.toYDB(),
			StorageSettings:      expect.StorageSettings.toYDB(),
		}

		d, err := s.DescribeTable(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(d, expect) {
			t.Fatalf("Result %+v differ from, expectd %+v", d, expect)
		}
	}
}

func TestSessionOperationModeOnExecuteDataQuery(t *testing.T) {
	fromTo := [...]struct {
		srcMode ydb.OperationMode
		dstMode ydb.OperationMode
	}{
		{
			srcMode: ydb.OperationModeUnknown,
			dstMode: ydb.OperationModeSync,
		},
		{
			srcMode: ydb.OperationModeSync,
			dstMode: ydb.OperationModeSync,
		},
		{
			srcMode: ydb.OperationModeAsync,
			dstMode: ydb.OperationModeAsync,
		},
	}
	for _, test := range []struct {
		method testutil.MethodCode
		do     func(t *testing.T, ctx context.Context, c Client)
	}{
		{
			method: testutil.TableExecuteDataQuery,
			do: func(t *testing.T, ctx context.Context, c Client) {
				s := &Session{
					c:      c,
					qcache: lru.New(0),
				}
				_, _, err := s.Execute(ctx, TxControl(), "", NewQueryParameters())
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableExplainDataQuery,
			do: func(t *testing.T, ctx context.Context, c Client) {
				s := &Session{
					c:      c,
					qcache: lru.New(0),
				}
				_, err := s.Explain(ctx, "")
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TablePrepareDataQuery,
			do: func(t *testing.T, ctx context.Context, c Client) {
				s := &Session{
					c:      c,
					qcache: lru.New(0),
				}
				_, err := s.Prepare(ctx, "")
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableCreateSession,
			do: func(t *testing.T, ctx context.Context, c Client) {
				_, err := c.CreateSession(ctx)
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableDeleteSession,
			do: func(t *testing.T, ctx context.Context, c Client) {
				s := &Session{
					c:      c,
					qcache: lru.New(0),
				}
				require.NoError(t, s.Close(ctx))
			},
		},
		{
			method: testutil.TableBeginTransaction,
			do: func(t *testing.T, ctx context.Context, c Client) {
				s := &Session{
					c:      c,
					qcache: lru.New(0),
				}
				_, err := s.BeginTransaction(ctx, TxSettings())
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableCommitTransaction,
			do: func(t *testing.T, ctx context.Context, c Client) {
				tx := &Transaction{
					s: &Session{
						c:      c,
						qcache: lru.New(0),
					},
				}
				_, err := tx.CommitTx(ctx)
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableRollbackTransaction,
			do: func(t *testing.T, ctx context.Context, c Client) {
				tx := &Transaction{
					s: &Session{
						c:      c,
						qcache: lru.New(0),
					},
				}
				err := tx.Rollback(ctx)
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableKeepAlive,
			do: func(t *testing.T, ctx context.Context, c Client) {
				s := &Session{
					c:      c,
					qcache: lru.New(0),
				}
				_, err := s.KeepAlive(ctx)
				require.NoError(t, err)
			},
		},
	} {
		t.Run(
			test.method.String(),
			func(t *testing.T) {
				for _, srcDst := range fromTo {
					t.Run(srcDst.srcMode.String()+"->"+srcDst.dstMode.String(), func(t *testing.T) {
						client := Client{
							Driver: &testutil.Driver{
								OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
									require.Equal(t, test.method, m)
									mode, ok := ydb.ContextOperationMode(ctx)
									require.True(t, ok)
									require.Equal(t, srcDst.dstMode, mode)
									switch m {
									case testutil.TableExecuteDataQuery:
										r := testutil.TableExecuteDataQueryResult{R: res}
										r.SetTransactionID("")
									case testutil.TableBeginTransaction:
										r := testutil.TableBeginTransactionResult{R: res}
										r.SetTransactionID("")
									}
									return nil
								},
							},
						}
						ctx, cancel := context.WithTimeout(
							context.Background(),
							time.Second,
						)
						if srcDst.srcMode != 0 {
							ctx = ydb.WithOperationMode(
								ctx,
								srcDst.srcMode,
							)
						}
						defer cancel()
						test.do(t, ctx, client)
					})
				}
			},
		)
	}
}

func TestClientCache(t *testing.T) {
	for _, test := range [...]struct {
		name                 string
		cacheSize            int
		prepareCount         int
		prepareRequestsCount int
	}{
		{
			name:                 "fixed query cache size, one request proxed to server",
			cacheSize:            10,
			prepareCount:         10,
			prepareRequestsCount: 1,
		},
		{
			name:                 "default query cache size, one request proxed to server",
			cacheSize:            0,
			prepareCount:         10,
			prepareRequestsCount: 1,
		},
		{
			name:                 "disabled query cache, all requests proxed to server",
			cacheSize:            -1,
			prepareCount:         10,
			prepareRequestsCount: 10,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepareRequestsCount := 0
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			client := &Client{
				Driver: &testutil.Driver{
					OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
						switch m {
						case testutil.TableCreateSession:
						case testutil.TablePrepareDataQuery:
							prepareRequestsCount++
						default:
							t.Fatalf("Unexpected method %d", m)

						}
						return nil
					},
				},
				MaxQueryCacheSize: test.cacheSize,
			}
			s, err := client.CreateSession(ctx)
			require.NoError(t, err)
			for i := 0; i < test.prepareCount; i++ {
				stmt, err := s.Prepare(ctx, "SELECT 1")
				require.NoError(t, err)
				require.NotNil(t, stmt)
			}
			require.Equal(t, test.prepareRequestsCount, prepareRequestsCount)
		})
	}
}
