//go:build cgo && oracle
// +build cgo,oracle

package common

import (
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/providers/oracle"
	_ "github.com/godror/godror"
	"github.com/jmoiron/sqlx"
)

//nolint:descriptiveerrors
func CreateConnection(config *oracle.OracleSource) (*sqlx.DB, error) {
	if connectionStr, err := GetConnectionString(config); err != nil {
		return nil, err
	} else {
		return sqlx.Open("godror", connectionStr)
	}
}
