package helpers

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/doublecloud/transfer/internal/logger"
	server "github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/connection"
	mysql "github.com/doublecloud/transfer/pkg/providers/mysql"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/log"
)

func RecipeMysqlSource() *mysql.MysqlSource {
	port, _ := strconv.Atoi(os.Getenv("RECIPE_MYSQL_PORT"))
	src := mysql.MysqlSource{
		Host:     os.Getenv("RECIPE_MYSQL_HOST"),
		User:     os.Getenv("RECIPE_MYSQL_USER"),
		Password: server.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database: os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:     port,
		ServerID: 1,
	}
	src.WithDefaults()
	return &src
}

func RecipeMysqlTarget() *mysql.MysqlDestination {
	port, _ := strconv.Atoi(os.Getenv("RECIPE_MYSQL_PORT"))
	v := mysql.MysqlDestination{
		Host:          os.Getenv("RECIPE_MYSQL_HOST"),
		User:          os.Getenv("RECIPE_MYSQL_USER"),
		Password:      server.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database:      os.Getenv("RECIPE_MYSQL_TARGET_DATABASE"),
		Port:          port,
		SkipKeyChecks: false,
	}
	v.WithDefaults()
	return &v
}

func RecipeMysqlSourceWithConnection(connID string) (*mysql.MysqlSource, *connection.ConnectionMySQL) {
	port, _ := strconv.Atoi(os.Getenv("RECIPE_MYSQL_PORT"))
	database := os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE")
	src := mysql.MysqlSource{
		ServerID:     1,
		Database:     database,
		ConnectionID: connID,
	}
	src.WithDefaults()

	managedConnection := ManagedConnection(port, os.Getenv("RECIPE_MYSQL_HOST"), database, os.Getenv("RECIPE_MYSQL_USER"), os.Getenv("RECIPE_MYSQL_PASSWORD"))
	return &src, managedConnection
}

func RecipeMysqlTargetWithConnection(connID string) (*mysql.MysqlDestination, *connection.ConnectionMySQL) {
	port, _ := strconv.Atoi(os.Getenv("RECIPE_MYSQL_PORT"))
	database := os.Getenv("RECIPE_MYSQL_TARGET_DATABASE")
	v := mysql.MysqlDestination{
		SkipKeyChecks: false,
		Database:      database,
		ConnectionID:  connID,
	}
	v.WithDefaults()

	managedConnection := ManagedConnection(port, os.Getenv("RECIPE_MYSQL_HOST"), database, os.Getenv("RECIPE_MYSQL_USER"), os.Getenv("RECIPE_MYSQL_PASSWORD"))
	return &v, managedConnection
}

func ManagedConnection(port int, host, dbName, user, password string) *connection.ConnectionMySQL {
	return &connection.ConnectionMySQL{
		BaseSQLConnection: &connection.BaseSQLConnection{
			Hosts:          []*connection.Host{{Name: host, Port: port, Role: connection.RoleUnknown, ReplicaType: connection.ReplicaUndefined}},
			User:           user,
			Password:       server.SecretString(password),
			Database:       dbName,
			HasTLS:         false,
			CACertificates: "",
			ClusterID:      "",
		},
		DatabaseNames: nil,
	}
}

func WithMysqlInclude(src *mysql.MysqlSource, regex []string) *mysql.MysqlSource {
	src.IncludeTableRegex = regex
	return src
}

func ExecuteMySQLStatement(t *testing.T, statement string, connectionParams *mysql.ConnectionParams) {
	conn, err := mysql.Connect(connectionParams, nil)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec(statement)
	require.NoError(t, err)
}

func ExecuteMySQLStatementsLineByLine(t *testing.T, statements string, connectionParams *mysql.ConnectionParams) {
	conn, err := mysql.Connect(connectionParams, nil)
	require.NoError(t, err)
	defer conn.Close()

	for _, line := range strings.Split(statements, "\n") {
		_, err = conn.Exec(line)
		if err != nil && !isEmptyQueryError(err) {
			require.Fail(t, err.Error())
		}
	}
}

func isEmptyQueryError(err error) bool {
	driverError, ok := err.(*mysqldriver.MySQLError)
	if !ok {
		return false
	}
	const emptyQueryErrorNumber = 1065
	return driverError.Number == emptyQueryErrorNumber
}

func MySQLDump(t *testing.T, storageParams *mysql.MysqlStorageParams) string {
	mysqlDumpPath := os.Getenv("RECIPE_MYSQLDUMP_BINARY")
	args := []string{
		"--host", storageParams.Host,
		"--user", storageParams.User,
		"--port", fmt.Sprintf("%d", storageParams.Port),
		"--databases", storageParams.Database,
		"--force",
		"--skip-extended-insert",
		"--dump-date=false",
		"--default-character-set=utf8mb4",
		fmt.Sprintf("--ignore-table=%s.__tm_gtid_keeper", storageParams.Database),
		fmt.Sprintf("--ignore-table=%s.__tm_keeper", storageParams.Database),
		fmt.Sprintf("--ignore-table=%s.__table_transfer_progress", storageParams.Database),
	}
	command := exec.Command(mysqlDumpPath, args...)
	command.Env = append(command.Env, fmt.Sprintf("MYSQL_PWD=%s", storageParams.Password))
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	logger.Log.Info("Run mysqldump", log.String("path", mysqlDumpPath), log.Array("args", args))
	require.NoError(t, command.Run(), stderr.String())
	logger.Log.Warnf("stderr\n%s", stderr.String())
	return stdout.String()
}

func NewMySQLConnectionParams(t *testing.T, storageParams *mysql.MysqlStorageParams) *mysql.ConnectionParams {
	connParams, err := mysql.NewConnectionParams(storageParams)
	require.NoError(t, err)
	return connParams
}

func NewMySQLStorageFromSource(t *testing.T, src *mysql.MysqlSource) *mysql.Storage {
	storage, err := mysql.NewStorage(src.ToStorageParams())
	require.NoError(t, err)
	return storage
}

func NewMySQLStorageFromTarget(t *testing.T, dst *mysql.MysqlDestination) *mysql.Storage {
	storage, err := mysql.NewStorage(dst.ToStorageParams())
	require.NoError(t, err)
	return storage
}
