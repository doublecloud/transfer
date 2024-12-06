package mysqlrecipe

import (
	"context"
	"os"
	"strconv"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract/model"
	"github.com/doublecloud/transfer/pkg/connection"
	"github.com/doublecloud/transfer/pkg/providers/mysql"
)

func Source() *mysql.MysqlSource {
	PrepareContainer(context.Background())
	port, _ := strconv.Atoi(os.Getenv("RECIPE_MYSQL_PORT"))
	src := mysql.MysqlSource{
		Host:     os.Getenv("RECIPE_MYSQL_HOST"),
		User:     os.Getenv("RECIPE_MYSQL_USER"),
		Password: model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database: os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:     port,
		ServerID: 1,
	}
	src.WithDefaults()
	return &src
}

func Target() *mysql.MysqlDestination {
	PrepareContainer(context.Background())
	port, _ := strconv.Atoi(os.Getenv("RECIPE_MYSQL_PORT"))
	v := mysql.MysqlDestination{
		Host:          os.Getenv("RECIPE_MYSQL_HOST"),
		User:          os.Getenv("RECIPE_MYSQL_USER"),
		Password:      model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database:      os.Getenv("RECIPE_MYSQL_TARGET_DATABASE"),
		Port:          port,
		SkipKeyChecks: false,
	}
	v.WithDefaults()
	return &v
}

func SourceWithConnection(connID string) (*mysql.MysqlSource, *connection.ConnectionMySQL) {
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

func TargetWithConnection(connID string) (*mysql.MysqlDestination, *connection.ConnectionMySQL) {
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
			Password:       model.SecretString(password),
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

func Exec(query string, connectionParams *mysql.ConnectionParams) error {
	conn, err := mysql.Connect(connectionParams, nil)
	if err != nil {
		return xerrors.Errorf("unable to build connection: %w", err)
	}
	_, err = conn.Exec(query)
	return err
}
