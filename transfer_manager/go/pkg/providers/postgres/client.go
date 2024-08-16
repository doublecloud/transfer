package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/internal/logger"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/dbaas"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/errors/coded"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/pgha"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.ytsaurus.tech/library/go/core/log"
)

const SelectCurrentLsnDelay = `select pg_wal_lsn_diff(pg_last_wal_replay_lsn(), $1);`

func ResolveReplicaHost(src *PgStorageParams) (string, error) {
	if src.ClusterID == "" {
		return "", nil
	}

	clusterHosts, err := dbaas.ResolveClusterHosts(dbaas.ProviderTypePostgresql, src.ClusterID)
	if err != nil {
		return "", xerrors.Errorf("unable to resolve postgres hosts: %w", err)
	}

	var masterHost string
	var aliveAsyncReplicas = make([]string, 0)
	var aliveSyncReplicas = make([]string, 0)
	for _, clusterHost := range clusterHosts {
		if clusterHost.Role == dbaas.MASTER {
			masterHost = clusterHost.Name
		} else if clusterHost.Health == dbaas.ALIVE {
			if clusterHost.ReplicaType == dbaas.ReplicaTypeAsync {
				aliveAsyncReplicas = append(aliveAsyncReplicas, clusterHost.Name)
			} else {
				aliveSyncReplicas = append(aliveSyncReplicas, clusterHost.Name)
			}
		}
	}

	if len(aliveAsyncReplicas) == 0 && len(aliveSyncReplicas) == 0 {
		logger.Log.Warn("No alive replicas found, will use master host instead")
		return "", nil
	}

	// in case we need replica for making a snapshot for Copy-and-replicate (CDC) transfer,
	// we need to make sure that replica host is up-to-date and has LSN not less than master's replication slot LSN
	// if no up-to-date replica was found we should keep using master host
	lsn, err := resolveSlotLSN(src, masterHost)
	if err != nil {
		return "", xerrors.Errorf("Failed to resolve lsn from master due to some error:  %w", err)
	}
	if lsn == "" {
		logger.Log.Info("Do not need to check replica's LSN before using - no cdc replication slot was found")
		return append(aliveAsyncReplicas, aliveSyncReplicas...)[0], nil
	}

	logger.Log.Info("Will check replica's LSN before using")
	// using async replica if available
	if len(aliveAsyncReplicas) > 0 {
		logger.Log.Info("Will check async replicas state first")
		host, err := findNonStaleReplica(src, lsn, aliveAsyncReplicas)
		if err != nil {
			logger.Log.Info("Error finding async replica host", log.Error(err))
		}
		if host != "" {
			return host, nil
		}
	}

	if len(aliveSyncReplicas) > 0 {
		logger.Log.Info("Checking sync replicas state")
		return findNonStaleReplica(src, lsn, aliveSyncReplicas)
	}

	return "", xerrors.Errorf("Could not find replica with Lsn greater than %v", lsn)
}

// Resolve slot LSN on master for current transfer_id
// see https://www.postgresql.org/docs/current/view-pg-replication-slots.html
func resolveSlotLSN(src *PgStorageParams, masterHost string) (string, error) {
	var lsn string

	conn, err := MakeConnPoolFromHostPort(src, masterHost, src.Port, logger.Log)
	if err != nil {
		logger.Log.Warn("Failed to make connection to master", log.Error(err))
		return "", xerrors.Errorf("Failed to make connection to master: %w", err)
	}
	defer conn.Close()

	if err = conn.QueryRow(context.TODO(), SelectLsnForSlot, src.SlotID).Scan(&lsn); err != nil {
		if err == pgx.ErrNoRows {
			logger.Log.Info("No replication slot found for transfer, will use any replica", log.Error(err))
			return "", nil
		}
		logger.Log.Warn("Failed to make get lsn to check replica status", log.Error(err))
		return "", xerrors.Errorf("Failed to get master lsn to check replica status, will use master instead: %w", err)
	}

	logger.Log.Infof("Master slot lsn is %v", lsn)
	return lsn, nil
}

// Choose replica host with LSN after master's slot LSN in order to make sure that no data will be lost
// between making snapshot and getting increments from WAL
// see pg_last_wal_replay_lsn https://www.postgresql.org/docs/15/functions-admin.html
func findNonStaleReplica(src *PgStorageParams, lsn string, aliveReplicas []string) (string, error) {
	var replicaHost string
	err := backoff.Retry(func() error {
		for _, replica := range aliveReplicas {
			if isReplicaUpToDate(src, replica, lsn) {
				replicaHost = replica
				return nil
			}
		}
		logger.Log.Infof("Could not find replica with Lsn greater than %v between hosts %v", lsn, aliveReplicas)
		return xerrors.Errorf("Could not find replica with Lsn greater than %v", lsn)
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))

	if err != nil {
		return "", err
	}
	logger.Log.Info("Found the fresh replicaHost!", log.String("replicaHost", replicaHost))
	return replicaHost, nil
}

func isReplicaUpToDate(src *PgStorageParams, replicaHost string, lsn string) bool {
	conn, err := MakeConnPoolFromHostPort(src, replicaHost, src.Port, logger.Log)
	if err != nil {
		logger.Log.Warn("Failed to make connection to replica", log.Error(err))
		return false
	}
	defer conn.Close()

	var replicaLsnDiff int

	if err = conn.QueryRow(context.TODO(), SelectCurrentLsnDelay, lsn).Scan(&replicaLsnDiff); err != nil {
		logger.Log.Warn("Failed to get replica wal lsn, will use master instead", log.Error(err))
		return false
	}

	logger.Log.Info("Got lsn diff for replica", log.String("replicaHost", replicaHost), log.Int("diff", replicaLsnDiff))
	return replicaLsnDiff >= 0
}

// ResolveMasterHostPortFrom*

func resolveMasterHostImplWithLog(lgr log.Logger, hosts []string, port int, hasTLS bool, database, user, password, clusterID, token string) (string, uint16, error) {
	masterHost, masterPort, err := resolveMasterHostImpl(hosts, port, hasTLS, database, user, password, clusterID, token)
	if err != nil {
		return "", 0, err
	}
	lgr.Infof("postgres master host/port: %s:%d", masterHost, masterPort)
	return masterHost, masterPort, nil
}

func resolveMasterHostImpl(hosts []string, port int, hasTLS bool, database, user, password, clusterID, token string) (string, uint16, error) {
	var pg *pgha.PgHA
	var err error

	if len(hosts) != 0 {
		// hosts
		if len(hosts) == 1 {
			// this function here is just for consistency - to any case supported port overriding
			resultHost, resultPort, err := dbaas.ResolveHostPortWithOverride(hosts[0], uint16(port))
			if err != nil {
				return "", 0, xerrors.Errorf("unable to parse host, err: %w", err)
			}
			return resultHost, resultPort, nil
		}
		pg, err = pgha.NewFromHosts(database, user, password, hosts, port, hasTLS)
		if err != nil {
			return "", 0, xerrors.Errorf("unable to create postgres service client from hosts: %w", err)
		}
		defer pg.Close()
	} else {
		// cluster
		pg, err = pgha.NewFromDBAAS(database, user, password, clusterID, token)
		if err != nil {
			return "", 0, xerrors.Errorf("unable to create postgres service client from clusterID: %w", err)
		}
		defer pg.Close()
	}

	masterNode, err := pg.MasterHost()
	if err != nil {
		return "", 0, xerrors.Errorf("unable to get master host: %w", err)
	}
	if masterNode == nil {
		return "", 0, xerrors.New("MasterHost() returned nil")
	}

	resultHost, resultPort, err := dbaas.ResolveHostPortWithOverride(*masterNode, uint16(port))
	if err != nil {
		return "", 0, xerrors.Errorf("unable to parse master host, err: %w", err)
	}
	return resultHost, resultPort, nil
}

func ResolveMasterHostPortFromSrc(lgr log.Logger, src *PgSource) (string, uint16, error) {
	return resolveMasterHostImplWithLog(lgr, src.AllHosts(), src.Port, src.HasTLS(), src.Database, src.User, string(src.Password), src.ClusterID, src.Token)
}

func ResolveMasterHostPortFromDst(lgr log.Logger, cfg *PgDestination) (string, uint16, error) {
	return resolveMasterHostImplWithLog(lgr, cfg.AllHosts(), cfg.Port, cfg.HasTLS(), cfg.Database, cfg.User, string(cfg.Password), cfg.ClusterID, cfg.Token)
}

func ResolveMasterHostPortFromStorage(lgr log.Logger, params *PgStorageParams) (string, uint16, error) {
	return resolveMasterHostImplWithLog(lgr, params.AllHosts, params.Port, params.HasTLS(), params.Database, params.User, params.Password, params.ClusterID, params.Token)
}

func ResolveMasterHostPortFromSink(lgr log.Logger, cfg PgSinkParams) (string, uint16, error) {
	return resolveMasterHostImplWithLog(lgr, cfg.AllHosts(), cfg.Port(), cfg.HasTLS(), cfg.Database(), cfg.User(), cfg.Password(), cfg.ClusterID(), cfg.Token())
}

// MakeConnConfigFrom*

func makeConnConfig(host string, port int, database, user, password, cluster string, hasTLS bool, tlsFile string) (*pgx.ConnConfig, error) {
	var tlsConfig *tls.Config
	if cluster != "" {
		tlsConfig = &tls.Config{
			ServerName: host,
		}
	} else if hasTLS {
		rootCertPool := x509.NewCertPool()
		if ok := rootCertPool.AppendCertsFromPEM([]byte(tlsFile)); !ok {
			return nil, xerrors.New("unable to add TLS to cert pool")
		}
		tlsConfig = &tls.Config{
			RootCAs:    rootCertPool,
			ServerName: host,
		}
	}

	config, _ := pgx.ParseConfig("")
	config.Host = host
	config.Port = uint16(port)
	config.Database = database
	config.User = user
	config.Password = password
	config.TLSConfig = tlsConfig
	config.PreferSimpleProtocol = true
	return config, nil
}

func MakeConnConfigFromSrc(lgr log.Logger, pgSrc *PgSource) (*pgx.ConnConfig, error) {
	masterHost, masterPort, err := ResolveMasterHostPortFromSrc(lgr, pgSrc)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve master host: %w", err)
	}
	return makeConnConfig(masterHost, int(masterPort), pgSrc.Database, pgSrc.User, string(pgSrc.Password), pgSrc.ClusterID, pgSrc.HasTLS(), pgSrc.TLSFile)
}

func MakeConnConfigFromSink(lgr log.Logger, params PgSinkParams) (*pgx.ConnConfig, error) {
	masterHost, masterPort, err := ResolveMasterHostPortFromSink(lgr, params)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve master host: %w", err)
	}
	return makeConnConfig(masterHost, int(masterPort), params.Database(), params.User(), params.Password(), params.ClusterID(), params.HasTLS(), params.TLSFile())
}

// MakeConnPoolFrom*

func MakeConnPoolFromSrc(pgSrc *PgSource, lgr log.Logger) (*pgxpool.Pool, error) {
	masterHost, masterPort, err := ResolveMasterHostPortFromSrc(lgr, pgSrc)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve master host from source: %w", err)
	}
	connConfig, err := makeConnConfig(masterHost, int(masterPort), pgSrc.Database, pgSrc.User, string(pgSrc.Password), pgSrc.ClusterID, pgSrc.HasTLS(), pgSrc.TLSFile)
	if err != nil {
		return nil, xerrors.Errorf("unable to make source connection config: %w", err)
	}
	return NewPgConnPool(connConfig, lgr)
}

func MakeConnPoolFromHostPort(cfg *PgStorageParams, host string, port int, lgr log.Logger) (*pgxpool.Pool, error) {
	logger.Log.Info("Using pg host to establish connection", log.String("pg_host", host))
	connConfig, err := makeConnConfig(host, port, cfg.Database, cfg.User, string(cfg.Password), cfg.ClusterID, cfg.TLSFile != "", cfg.TLSFile)
	if err != nil {
		return nil, xerrors.Errorf("unable to make source connection config: %w", err)
	}
	return NewPgConnPool(connConfig, lgr)
}

func MakeConnPoolFromDst(pgDst *PgDestination, lgr log.Logger) (*pgxpool.Pool, error) {
	masterHost, masterPort, err := ResolveMasterHostPortFromDst(lgr, pgDst)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve master host from destination: %w", err)
	}
	connConfig, err := makeConnConfig(masterHost, int(masterPort), pgDst.Database, pgDst.User, string(pgDst.Password), pgDst.ClusterID, pgDst.HasTLS(), pgDst.TLSFile)
	if err != nil {
		return nil, xerrors.Errorf("unable to make destination connection config: %w", err)
	}
	return NewPgConnPool(connConfig, lgr)
}

func NewPgConnPool(connConfig *pgx.ConnConfig, lgr log.Logger, dataTypesOptions ...DataTypesOption) (*pgxpool.Pool, error) {
	connConfig.PreferSimpleProtocol = true

	connConfigCopy := connConfig
	if lgr == nil {
		connConfigCopy.Logger = nil
	} else {
		connConfigCopy = WithLogger(connConfig, log.With(lgr, log.Any("component", "pgx")))
	}

	poolConfig, _ := pgxpool.ParseConfig("")
	poolConfig.ConnConfig = connConfigCopy
	poolConfig.MaxConns = 5
	poolConfig.AfterConnect = MakeInitDataTypes(dataTypesOptions...)

	pool, err := NewPgConnPoolConfig(context.TODO(), poolConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to make a connection pool: %w", err)
	}
	return pool, nil
}

// pgStatementTimeout returns the GUC `statement_timeout` or a default timeout if the former is zero
func pgStatementTimeout(ctx context.Context, conn *pgx.Conn) (time.Duration, error) {
	sql := "select setting from pg_settings where name = 'statement_timeout'"
	var rawResult string
	if err := conn.QueryRow(ctx, sql).Scan(&rawResult); err != nil {
		return 0, xerrors.Errorf("failed to execute SQL '%v': %w", sql, err)
	}
	rawResultParsed, err := strconv.ParseInt(rawResult, 10, 64)
	if err != nil {
		return 0, xerrors.Errorf("failed to parse statement_timeout: %w", err)
	}
	result := time.Duration(rawResultParsed)

	if result <= 0 {
		return 5 * time.Minute, nil
	} else {
		return result * time.Millisecond, nil
	}
}

// NewPgConnPoolConfig creates a connection pool. It provides built-in timeouts to limit the duration of connection attempts
func NewPgConnPoolConfig(ctx context.Context, poolConfig *pgxpool.Config) (*pgxpool.Pool, error) {
	basicCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	pgxConn, err := pgx.ConnectConfig(basicCtx, poolConfig.ConnConfig)
	if err != nil {
		if IsPgError(err, ErrcInvalidPassword) || IsPgError(err, ErrcInvalidAuthSpec) {
			return nil, coded.Errorf(providers.InvalidCredential, "failed to connect to a PostgreSQL instance: %w", err)
		}
		return nil, xerrors.Errorf("failed to connect to a PostgreSQL instance: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		if err := pgxConn.Close(closeCtx); err != nil {
			logger.Log.Error("Failed to close connection used to obtain timeout", log.Error(err))
		}
	}()
	statementTimeout, err := pgStatementTimeout(basicCtx, pgxConn)
	if err != nil {
		return nil, xerrors.Errorf("failed to get statement timeout from a PostgreSQL instance: %w", err)
	}
	cancel()

	goodTimeoutCtx, cancel := context.WithTimeout(ctx, 5*statementTimeout)
	defer cancel()
	result, err := pgxpool.ConnectConfig(goodTimeoutCtx, poolConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to a PostgreSQL instance or create a connection pool: %w", err)
	}

	return result, nil
}
