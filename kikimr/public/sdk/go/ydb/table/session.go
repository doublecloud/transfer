package table

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/internal/cache/lru"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
)

// Deprecated: use server-side query cache with keep-in-memory flag control instead
var DefaultMaxQueryCacheSize = 1000

// Client contains logic of creation of ydb table sessions.
type Client struct {
	Driver ydb.Driver
	Trace  ClientTrace

	// Deprecated: use server-side query cache with keep-in-memory flag control instead
	// MaxQueryCacheSize limits maximum number of queries which able to live in
	// cache. Note that cache is not shared across sessions.
	//
	// If MaxQueryCacheSize equal to zero, then the DefaultMaxQueryCacheSize is used.
	//
	// If MaxQueryCacheSize is less than zero, then client not used query cache.
	MaxQueryCacheSize int
}

// CreateSession creates new session instance.
// Unused sessions must be destroyed.
func (t *Client) CreateSession(ctx context.Context) (s *Session, err error) {
	clientTraceCreateSessionDone := clientTraceOnCreateSession(ctx, t.Trace, ctx)
	start := time.Now()
	defer func() {
		endpoint := ""
		if s != nil && s.endpointInfo != nil {
			endpoint = s.endpointInfo.Address()
		}
		clientTraceCreateSessionDone(ctx, s, endpoint, time.Since(start), err)
	}()
	var (
		req Ydb_Table.CreateSessionRequest
		res Ydb_Table.CreateSessionResult
	)
	var endpointInfo ydb.EndpointInfo
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	endpointInfo, err = t.Driver.Call(
		ctx,
		internal.Wrap(
			"/Ydb.Table.V1.TableService/CreateSession",
			&req,
			&res,
		),
	)
	if err != nil {
		return nil, err
	}
	s = &Session{
		ID:           res.SessionId,
		endpointInfo: endpointInfo,
		c:            *t,
		qcache:       lru.New(t.cacheSize()),
	}
	return
}

func (t *Client) cacheSize() int {
	if t.MaxQueryCacheSize == 0 {
		return DefaultMaxQueryCacheSize
	}
	return t.MaxQueryCacheSize
}

// Session represents a single table API session.
//
// Session methods are not goroutine safe. Simultaneous execution of requests
// are forbidden within a single session.
//
// Note that after Session is no longer needed it should be destroyed by
// Close() call.
type Session struct {
	ID           string
	endpointInfo ydb.EndpointInfo

	c Client

	qcache lru.Cache
	qhash  queryHasher

	closeMux sync.Mutex
	closed   bool
	onClose  []func()
}

func (s *Session) OnClose(cb func()) {
	s.closeMux.Lock()
	defer s.closeMux.Unlock()
	if s.closed {
		return
	}
	s.onClose = append(s.onClose, cb)
}

func (s *Session) Close(ctx context.Context) (err error) {
	s.closeMux.Lock()
	defer s.closeMux.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	clientTraceDeleteSessionDone := clientTraceOnDeleteSession(ctx, s.c.Trace, ctx, s)
	// call all close listeners before doing request
	// firstly this need to clear pool from this session
	for _, cb := range s.onClose {
		cb()
	}
	start := time.Now()
	defer func() {
		clientTraceDeleteSessionDone(ctx, s, time.Since(start), err)
	}()
	req := Ydb_Table.DeleteSessionRequest{
		SessionId: s.ID,
	}
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/DeleteSession",
			&req,
			nil,
		),
	)
	return err
}

func (s *Session) Address() string {
	if s.endpointInfo != nil {
		return s.endpointInfo.Address()
	}
	return ""
}

// KeepAlive keeps idle session alive.
func (s *Session) KeepAlive(ctx context.Context) (info SessionInfo, err error) {
	clientTraceKeepAliveDone := clientTraceOnKeepAlive(ctx, s.c.Trace, ctx, s)
	defer func() {
		clientTraceKeepAliveDone(ctx, s, info, err)
	}()
	var res Ydb_Table.KeepAliveResult
	req := Ydb_Table.KeepAliveRequest{
		SessionId: s.ID,
	}
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/KeepAlive",
			&req,
			&res,
		),
	)
	if err != nil {
		return
	}
	switch res.SessionStatus {
	case Ydb_Table.KeepAliveResult_SESSION_STATUS_READY:
		info.Status = SessionReady
	case Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY:
		info.Status = SessionBusy
	}
	return
}

// CreateTable creates table at given path with given options.
func (s *Session) CreateTable(ctx context.Context, path string, opts ...CreateTableOption) (err error) {
	req := Ydb_Table.CreateTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*createTableDesc)(&req))
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/CreateTable",
			&req,
			nil,
		),
	)
	return err
}

// DescribeTable describes table at given path.
func (s *Session) DescribeTable(ctx context.Context, path string, opts ...DescribeTableOption) (desc Description, err error) {
	var res Ydb_Table.DescribeTableResult
	req := Ydb_Table.DescribeTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*describeTableDesc)(&req))
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/DescribeTable",
			&req,
			&res,
		),
	)
	if err != nil {
		return desc, err
	}

	cs := make([]Column, len(res.Columns))
	for i, c := range res.Columns {
		cs[i] = Column{
			Name:   c.Name,
			Type:   internal.TypeFromYDB(c.Type),
			Family: c.Family,
		}
	}

	rs := make([]KeyRange, len(res.ShardKeyBounds)+1)
	var last ydb.Value
	for i, b := range res.ShardKeyBounds {
		if last != nil {
			rs[i].From = last
		}

		bound := internal.ValueFromYDB(b.Type, b.Value)
		rs[i].To = bound

		last = bound
	}
	if last != nil {
		i := len(rs) - 1
		rs[i].From = last
	}

	var stats *TableStats
	if res.TableStats != nil {
		resStats := res.TableStats
		partStats := make([]PartitionStats, len(res.TableStats.PartitionStats))
		for i, v := range res.TableStats.PartitionStats {
			partStats[i].RowsEstimate = v.RowsEstimate
			partStats[i].StoreSize = v.StoreSize
		}
		var creationTime, modificationTime time.Time
		if resStats.CreationTime.GetSeconds() != 0 {
			creationTime = time.Unix(resStats.CreationTime.GetSeconds(), int64(resStats.CreationTime.GetNanos()))
		}
		if resStats.ModificationTime.GetSeconds() != 0 {
			modificationTime = time.Unix(resStats.ModificationTime.GetSeconds(), int64(resStats.ModificationTime.GetNanos()))
		}

		stats = &TableStats{
			PartitionStats:   partStats,
			RowsEstimate:     resStats.RowsEstimate,
			StoreSize:        resStats.StoreSize,
			Partitions:       resStats.Partitions,
			CreationTime:     creationTime,
			ModificationTime: modificationTime,
		}
	}

	cf := make([]ColumnFamily, len(res.ColumnFamilies))
	for i, c := range res.ColumnFamilies {
		cf[i] = columnFamily(c)
	}

	attrs := make(map[string]string, len(res.Attributes))
	for k, v := range res.Attributes {
		attrs[k] = v
	}

	indexes := make([]IndexDescription, len(res.Indexes))
	for i, idx := range res.Indexes {
		indexes[i] = IndexDescription{
			Name:         idx.Name,
			IndexColumns: idx.IndexColumns,
			Status:       idx.Status,
		}
	}

	return Description{
		Name:                 res.Self.Name,
		PrimaryKey:           res.PrimaryKey,
		Columns:              cs,
		KeyRanges:            rs,
		Stats:                stats,
		ColumnFamilies:       cf,
		Attributes:           attrs,
		ReadReplicaSettings:  readReplicasSettings(res.GetReadReplicasSettings()),
		StorageSettings:      storageSettings(res.GetStorageSettings()),
		KeyBloomFilter:       internal.FeatureFlagFromYDB(res.GetKeyBloomFilter()),
		PartitioningSettings: partitioningSettings(res.GetPartitioningSettings()),
		Indexes:              indexes,
		TTLSettings:          ttlSettings(res.GetTtlSettings()),
		TimeToLiveSettings:   timeToLiveSettings(res.GetTtlSettings()),
	}, nil
}

// DropTable drops table at given path with given options.
func (s *Session) DropTable(ctx context.Context, path string, opts ...DropTableOption) (err error) {
	req := Ydb_Table.DropTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*dropTableDesc)(&req))
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/DropTable",
			&req,
			nil,
		),
	)
	return err
}

// AlterTable modifies schema of table at given path with given options.
func (s *Session) AlterTable(ctx context.Context, path string, opts ...AlterTableOption) (err error) {
	req := Ydb_Table.AlterTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*alterTableDesc)(&req))
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/AlterTable",
			&req,
			nil,
		),
	)
	return err
}

// CopyTable creates copy of table at given path.
func (s *Session) CopyTable(ctx context.Context, dst, src string, _ ...CopyTableOption) (err error) {
	req := Ydb_Table.CopyTableRequest{
		SessionId:       s.ID,
		SourcePath:      src,
		DestinationPath: dst,
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/CopyTable",
			&req,
			nil,
		),
	)
	return err
}

// DataQueryExplanation is a result of ExplainDataQuery call.
type DataQueryExplanation struct {
	AST  string
	Plan string
}

// Explain explains data query represented by text.
func (s *Session) Explain(ctx context.Context, query string) (exp DataQueryExplanation, err error) {
	var res Ydb_Table.ExplainQueryResult
	req := Ydb_Table.ExplainDataQueryRequest{
		SessionId: s.ID,
		YqlText:   query,
	}
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/ExplainDataQuery",
			&req,
			&res,
		),
	)
	if err != nil {
		return
	}
	return DataQueryExplanation{
		AST:  res.QueryAst,
		Plan: res.QueryPlan,
	}, nil
}

// Statement is a prepared statement. Like a single Session, it is not safe for
// concurrent use by multiple goroutines.
type Statement struct {
	session *Session
	query   *DataQuery
	qhash   queryHash
	params  map[string]*Ydb.Type
}

// Execute executes prepared data query.
func (s *Statement) Execute(
	ctx context.Context, tx *TransactionControl,
	params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (
	txr *Transaction, r *Result, err error,
) {
	clientTraceExecuteDataQueryDone := clientTraceOnExecuteDataQuery(ctx, s.session.c.Trace, ctx, s.session, tx.id(), s.query, params)
	defer func() {
		clientTraceExecuteDataQueryDone(ctx, s.session, GetTransactionID(txr), s.query, params, true, r, err)
	}()
	return s.execute(ctx, tx, params, opts...)
}

// execute executes prepared query without any tracing.
func (s *Statement) execute(
	ctx context.Context, tx *TransactionControl,
	params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (
	txr *Transaction, r *Result, err error,
) {
	_, res, err := s.session.executeDataQuery(ctx, tx, s.query, params, opts...)
	if ydb.IsOpError(err, ydb.StatusNotFound) {
		s.session.qcache.Remove(s.qhash)
	}
	if err != nil {
		return nil, nil, err
	}
	return s.session.executeQueryResult(res)
}

func (s *Statement) NumInput() int {
	return len(s.params)
}

func (s *Statement) Text() string {
	return s.query.YQL()
}

// Prepare prepares data query within session s.
func (s *Session) Prepare(
	ctx context.Context, query string,
) (
	stmt *Statement, err error,
) {
	var (
		cached bool
		q      *DataQuery
	)
	clientTracePrepareDataQueryDone := clientTraceOnPrepareDataQuery(ctx, s.c.Trace, ctx, s, query)
	defer func() {
		clientTracePrepareDataQueryDone(ctx, s, query, q, cached, err)
	}()

	cacheKey := s.qhash.hash(query)
	stmt, cached = s.getQueryFromCache(cacheKey)
	if cached {
		q = stmt.query
		return stmt, nil
	}

	var res Ydb_Table.PrepareQueryResult
	req := Ydb_Table.PrepareDataQueryRequest{
		SessionId: s.ID,
		YqlText:   query,
	}
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/PrepareDataQuery",
			&req,
			&res,
		),
	)
	if err != nil {
		return nil, err
	}

	q = new(DataQuery)
	q.initPrepared(res.QueryId)
	stmt = &Statement{
		session: s,
		query:   q,
		qhash:   cacheKey,
		params:  res.ParametersTypes,
	}
	s.addQueryToCache(cacheKey, stmt)

	return stmt, nil
}

func (s *Session) getQueryFromCache(key queryHash) (*Statement, bool) {
	v, cached := s.qcache.Get(key)
	if cached {
		return v.(*Statement), true
	}
	return nil, false
}

func (s *Session) addQueryToCache(key queryHash, stmt *Statement) {
	s.qcache.Add(key, stmt)
}

// Execute executes given data query represented by text.
func (s *Session) Execute(
	ctx context.Context,
	tx *TransactionControl,
	query string,
	params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (
	txr *Transaction, r *Result, err error,
) {
	q := new(DataQuery)
	q.initFromText(query)

	var cached bool
	clientTraceExecuteDataQueryDone := clientTraceOnExecuteDataQuery(ctx, s.c.Trace, ctx, s, tx.id(), q, params)
	defer func() {
		clientTraceExecuteDataQueryDone(ctx, s, GetTransactionID(txr), q, params, true, r, err)
	}()

	cacheKey := s.qhash.hash(query)
	stmt, cached := s.getQueryFromCache(cacheKey)
	if cached {
		// Supplement q with ID for tracing.
		q.initPreparedText(query, stmt.query.ID())
		return stmt.execute(ctx, tx, params, opts...)
	}
	req, res, err := s.executeDataQuery(ctx, tx, q, params, opts...)
	if err != nil {
		return nil, nil, err
	}
	if keepInCache(req) && res.QueryMeta != nil {
		queryID := res.QueryMeta.Id
		// Supplement q with ID for tracing.
		q.initPreparedText(query, queryID)
		// Create new DataQuery instead of q above to not store the whole query
		// string within statement.
		subq := new(DataQuery)
		subq.initPrepared(queryID)
		stmt = &Statement{
			session: s,
			query:   subq,
			qhash:   cacheKey,
			params:  res.QueryMeta.ParametersTypes,
		}
		s.addQueryToCache(cacheKey, stmt)
	}

	return s.executeQueryResult(res)
}

func keepInCache(req *Ydb_Table.ExecuteDataQueryRequest) bool {
	p := req.QueryCachePolicy
	return p != nil && p.KeepInCache
}

// executeQueryResult returns Transaction and Result built from received
// result.
func (s *Session) executeQueryResult(res *Ydb_Table.ExecuteQueryResult) (*Transaction, *Result, error) {
	t := &Transaction{
		id: res.TxMeta.Id,
		s:  s,
	}
	r := &Result{
		sets:  res.ResultSets,
		stats: res.QueryStats,
	}
	return t, r, nil
}

// executeDataQuery executes data query.
func (s *Session) executeDataQuery(
	ctx context.Context, tx *TransactionControl,
	query *DataQuery, params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (
	req *Ydb_Table.ExecuteDataQueryRequest,
	res *Ydb_Table.ExecuteQueryResult,
	err error,
) {
	res = new(Ydb_Table.ExecuteQueryResult)
	req = &Ydb_Table.ExecuteDataQueryRequest{
		SessionId:  s.ID,
		TxControl:  &tx.desc,
		Parameters: params.params(),
		Query:      &query.query,
	}
	for _, opt := range opts {
		opt((*executeDataQueryDesc)(req))
	}
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/ExecuteDataQuery",
			req,
			res,
		),
	)
	return
}

// ExecuteSchemeQuery executes scheme query.
func (s *Session) ExecuteSchemeQuery(
	ctx context.Context, query string,
	opts ...ExecuteSchemeQueryOption,
) (err error) {
	req := Ydb_Table.ExecuteSchemeQueryRequest{
		SessionId: s.ID,
		YqlText:   query,
	}
	for _, opt := range opts {
		opt((*executeSchemeQueryDesc)(&req))
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/ExecuteSchemeQuery",
			&req,
			nil,
		),
	)
	return err
}

// DescribeTableOptions describes supported table options.
func (s *Session) DescribeTableOptions(ctx context.Context) (desc TableOptionsDescription, err error) {
	var res Ydb_Table.DescribeTableOptionsResult
	req := Ydb_Table.DescribeTableOptionsRequest{}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/DescribeTableOptions",
			&req,
			&res,
		),
	)
	if err != nil {
		return
	}
	{
		xs := make([]TableProfileDescription, len(res.TableProfilePresets))
		for i, p := range res.TableProfilePresets {
			xs[i] = TableProfileDescription{
				Name:   p.Name,
				Labels: p.Labels,

				DefaultStoragePolicy:      p.DefaultStoragePolicy,
				DefaultCompactionPolicy:   p.DefaultCompactionPolicy,
				DefaultPartitioningPolicy: p.DefaultPartitioningPolicy,
				DefaultExecutionPolicy:    p.DefaultExecutionPolicy,
				DefaultReplicationPolicy:  p.DefaultReplicationPolicy,
				DefaultCachingPolicy:      p.DefaultCachingPolicy,

				AllowedStoragePolicies:      p.AllowedStoragePolicies,
				AllowedCompactionPolicies:   p.AllowedCompactionPolicies,
				AllowedPartitioningPolicies: p.AllowedPartitioningPolicies,
				AllowedExecutionPolicies:    p.AllowedExecutionPolicies,
				AllowedReplicationPolicies:  p.AllowedReplicationPolicies,
				AllowedCachingPolicies:      p.AllowedCachingPolicies,
			}
		}
		desc.TableProfilePresets = xs
	}
	{
		xs := make([]StoragePolicyDescription, len(res.StoragePolicyPresets))
		for i, p := range res.StoragePolicyPresets {
			xs[i] = StoragePolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.StoragePolicyPresets = xs
	}
	{
		xs := make([]CompactionPolicyDescription, len(res.CompactionPolicyPresets))
		for i, p := range res.CompactionPolicyPresets {
			xs[i] = CompactionPolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.CompactionPolicyPresets = xs
	}
	{
		xs := make([]PartitioningPolicyDescription, len(res.PartitioningPolicyPresets))
		for i, p := range res.PartitioningPolicyPresets {
			xs[i] = PartitioningPolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.PartitioningPolicyPresets = xs
	}
	{
		xs := make([]ExecutionPolicyDescription, len(res.ExecutionPolicyPresets))
		for i, p := range res.ExecutionPolicyPresets {
			xs[i] = ExecutionPolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.ExecutionPolicyPresets = xs
	}
	{
		xs := make([]ReplicationPolicyDescription, len(res.ReplicationPolicyPresets))
		for i, p := range res.ReplicationPolicyPresets {
			xs[i] = ReplicationPolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.ReplicationPolicyPresets = xs
	}
	{
		xs := make([]CachingPolicyDescription, len(res.CachingPolicyPresets))
		for i, p := range res.CachingPolicyPresets {
			xs[i] = CachingPolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.CachingPolicyPresets = xs
	}
	return desc, nil
}

// StreamReadTable reads table at given path with given options.
//
// Note that given ctx controls the lifetime of the whole read, not only this
// StreamReadTable() call; that is, the time until returned result is closed
// via Close() call or fully drained by sequential NextStreamSet() calls.
func (s *Session) StreamReadTable(ctx context.Context, path string, opts ...ReadTableOption) (r *Result, err error) {
	clientTraceStreamReadTableDone := clientTraceOnStreamReadTable(ctx, s.c.Trace, ctx, s)
	defer func() {
		clientTraceStreamReadTableDone(ctx, s, r, err)
	}()

	var resp Ydb_Table.ReadTableResponse
	req := Ydb_Table.ReadTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*readTableDesc)(&req))
	}

	ctx, cancel := context.WithCancel(ctx)
	var (
		ch   = make(chan *Ydb.ResultSet, 1)
		ce   = new(error)
		once = sync.Once{}
	)
	_, err = s.c.Driver.StreamRead(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.WrapStreamOperation(
			"/Ydb.Table.V1.TableService/StreamReadTable",
			&req,
			&resp,
			func(err error) {
				if err != io.EOF {
					*ce = err
				}
				if err != nil {
					once.Do(func() { close(ch) })
					return
				}
				select {
				case <-ctx.Done():
					once.Do(func() { close(ch) })
				default:
					if result := resp.Result; result != nil {
						if result.ResultSet != nil {
							ch <- resp.Result.ResultSet
						}
					}
				}
			},
		),
	)
	if err != nil {
		cancel()
		return
	}
	r = &Result{
		setCh:       ch,
		setChErr:    ce,
		setChCancel: cancel,
	}
	return r, nil
}

// StreamExecuteScanQuery scan-reads table at given path with given options.
//
// Note that given ctx controls the lifetime of the whole read, not only this
// StreamExecuteScanQuery() call; that is, the time until returned result is closed
// via Close() call or fully drained by sequential NextStreamSet() calls.
func (s *Session) StreamExecuteScanQuery(
	ctx context.Context,
	query string,
	params *QueryParameters,
	opts ...ExecuteScanQueryOption,
) (
	_ *Result, err error,
) {
	q := new(DataQuery)
	q.initFromText(query)
	var resp Ydb_Table.ExecuteScanQueryPartialResponse
	req := Ydb_Table.ExecuteScanQueryRequest{
		Query:      &q.query,
		Parameters: params.params(),
		Mode:       Ydb_Table.ExecuteScanQueryRequest_MODE_EXEC, // set default
	}
	for _, opt := range opts {
		opt((*executeScanQueryDesc)(&req))
	}

	ctx, cancel := context.WithCancel(ctx)
	var (
		once = sync.Once{}
		r    = &Result{
			setCh:       make(chan *Ydb.ResultSet, 1),
			setChCancel: cancel,
		}
	)

	clientTraceStreamExecuteScanQueryDone := clientTraceOnStreamExecuteScanQuery(ctx, s.c.Trace, ctx, s, q, params)
	defer func() {
		clientTraceStreamExecuteScanQueryDone(ctx, s, q, params, r, err)
	}()

	_, err = s.c.Driver.StreamRead(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.WrapStreamOperation(
			"/Ydb.Table.V1.TableService/StreamExecuteScanQuery",
			&req,
			&resp,
			func(err error) {
				if err != io.EOF {
					r.setChErr = &err
				}
				if err != nil {
					once.Do(func() { close(r.setCh) })
					return
				}
				select {
				case <-ctx.Done():
					once.Do(func() {
						err = ctx.Err()
						r.setChErr = &err
						close(r.setCh)
					})
				default:
					if result := resp.Result; result != nil {
						if result.ResultSet != nil {
							r.setCh <- resp.Result.ResultSet
						}
						// TODO: something
						// if result.QueryStats != nil {
						// }
					}
				}
			},
		),
	)
	if err != nil {
		cancel()
		return
	}
	return r, nil
}

// BulkUpsert uploads given list of ydb struct values to the table.
func (s *Session) BulkUpsert(ctx context.Context, table string, rows ydb.Value) (err error) {
	req := Ydb_Table.BulkUpsertRequest{
		Table: table,
		Rows:  internal.ValueToYDB(rows),
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/BulkUpsert",
			&req, nil,
		),
	)
	return err
}

// BeginTransaction begins new transaction within given session with given
// settings.
func (s *Session) BeginTransaction(ctx context.Context, tx *TransactionSettings) (x *Transaction, err error) {
	clientTraceBeginTransactionDone := clientTraceOnBeginTransaction(ctx, s.c.Trace, ctx, s)
	defer func() {
		clientTraceBeginTransactionDone(ctx, s, GetTransactionID(x), err)
	}()
	var res Ydb_Table.BeginTransactionResult
	req := Ydb_Table.BeginTransactionRequest{
		SessionId:  s.ID,
		TxSettings: &tx.settings,
	}
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	_, err = s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/BeginTransaction",
			&req,
			&res,
		),
	)
	if err != nil {
		return
	}
	return &Transaction{
		id: res.TxMeta.Id,
		s:  s,
	}, nil
}

// Transaction is a database transaction.
// Hence session methods are not goroutine safe, Transaction is not goroutine
// safe either.
type Transaction struct {
	id string
	s  *Session
	c  *TransactionControl
}

// Execute executes query represented by text within transaction tx.
func (tx *Transaction) Execute(
	ctx context.Context,
	query string, params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (r *Result, err error) {
	_, r, err = tx.s.Execute(ctx, tx.txc(), query, params, opts...)
	return
}

// ExecuteStatement executes prepared statement stmt within transaction tx.
func (tx *Transaction) ExecuteStatement(
	ctx context.Context,
	stmt *Statement, params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (r *Result, err error) {
	_, r, err = stmt.Execute(ctx, tx.txc(), params, opts...)
	return
}

// Deprecated: Use CommitTx instead
// Commit commits specified active transaction.
func (tx *Transaction) Commit(ctx context.Context) (err error) {
	clientTraceCommitTransactionDone := clientTraceOnCommitTransaction(ctx, tx.s.c.Trace, ctx, tx.s, tx.id)
	defer func() {
		clientTraceCommitTransactionDone(ctx, tx.s, tx.id, err)
	}()
	req := Ydb_Table.CommitTransactionRequest{
		SessionId: tx.s.ID,
		TxId:      tx.id,
	}
	_, err = tx.s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			tx.s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/CommitTransaction",
			&req,
			nil,
		),
	)
	return err
}

// CommitTx commits specified active transaction.
func (tx *Transaction) CommitTx(ctx context.Context, opts ...CommitTransactionOption) (result *Result, err error) {
	clientTraceCommitTransactionDone := clientTraceOnCommitTransaction(ctx, tx.s.c.Trace, ctx, tx.s, tx.id)
	defer func() {
		clientTraceCommitTransactionDone(ctx, tx.s, tx.id, err)
	}()
	res := new(Ydb_Table.CommitTransactionResult)
	req := &Ydb_Table.CommitTransactionRequest{
		SessionId: tx.s.ID,
		TxId:      tx.id,
	}
	for _, opt := range opts {
		opt((*commitTransactionDesc)(req))
	}
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	_, err = tx.s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			tx.s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/CommitTransaction",
			req,
			res,
		),
	)
	return &Result{stats: res.QueryStats}, err
}

// Rollback performs a rollback of the specified active transaction.
func (tx *Transaction) Rollback(ctx context.Context) (err error) {
	clientTraceRollbackTransactionDone := clientTraceOnRollbackTransaction(ctx, tx.s.c.Trace, ctx, tx.s, tx.id)
	defer func() {
		clientTraceRollbackTransactionDone(ctx, tx.s, tx.id, err)
	}()
	req := Ydb_Table.RollbackTransactionRequest{
		SessionId: tx.s.ID,
		TxId:      tx.id,
	}
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	_, err = tx.s.c.Driver.Call(
		ydb.WithEndpointInfo(
			ctx,
			tx.s.endpointInfo,
		),
		internal.Wrap(
			"/Ydb.Table.V1.TableService/RollbackTransaction",
			&req,
			nil,
		),
	)
	return err
}

func (tx *Transaction) txc() *TransactionControl {
	if tx.c == nil {
		tx.c = TxControl(WithTx(tx))
	}
	return tx.c
}

type DataQuery struct {
	query    Ydb_Table.Query
	queryID  Ydb_Table.Query_Id
	queryYQL Ydb_Table.Query_YqlText
}

func (q *DataQuery) String() string {
	var emptyID Ydb_Table.Query_Id
	if q.queryID == emptyID {
		return q.queryYQL.YqlText
	}
	return q.queryID.Id
}

func (q *DataQuery) ID() string {
	return q.queryID.Id
}

func (q *DataQuery) YQL() string {
	return q.queryYQL.YqlText
}

func (q *DataQuery) initFromText(s string) {
	q.queryID = Ydb_Table.Query_Id{} // Reset id field.
	q.queryYQL.YqlText = s
	q.query.Query = &q.queryYQL
}

func (q *DataQuery) initPrepared(id string) {
	q.queryYQL = Ydb_Table.Query_YqlText{} // Reset yql field.
	q.queryID.Id = id
	q.query.Query = &q.queryID
}

func (q *DataQuery) initPreparedText(s, id string) {
	q.queryYQL = Ydb_Table.Query_YqlText{} // Reset yql field.
	q.queryYQL.YqlText = s

	q.queryID = Ydb_Table.Query_Id{} // Reset id field.
	q.queryID.Id = id

	q.query.Query = &q.queryID // Prefer preared query.
}

type QueryParameters struct {
	m queryParams
}

func (q *QueryParameters) params() queryParams {
	if q == nil {
		return nil
	}
	return q.m
}

func (q *QueryParameters) Each(it func(name string, value ydb.Value)) {
	if q == nil {
		return
	}
	for key, value := range q.m {
		it(key, internal.ValueFromYDB(
			value.Type,
			value.Value,
		))
	}
}

func (q *QueryParameters) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	q.Each(func(name string, value ydb.Value) {
		buf.WriteString("((")
		buf.WriteString(name)
		buf.WriteByte(')')
		buf.WriteByte('(')
		internal.WriteValueStringTo(&buf, value)
		buf.WriteString("))")
	})
	buf.WriteByte(')')
	return buf.String()
}

type queryParams map[string]*Ydb.TypedValue

type ParameterOption func(queryParams)

func NewQueryParameters(opts ...ParameterOption) *QueryParameters {
	q := &QueryParameters{
		m: make(queryParams, len(opts)),
	}
	q.Add(opts...)
	return q
}

func (q *QueryParameters) Add(opts ...ParameterOption) {
	for _, opt := range opts {
		opt(q.m)
	}
}

func ValueParam(name string, v ydb.Value) ParameterOption {
	return func(q queryParams) {
		q[name] = internal.ValueToYDB(v)
	}
}

func GetTransactionID(txr *Transaction) string {
	if txr != nil {
		return txr.id
	}
	return ""
}
