package ydb

import (
	"github.com/doublecloud/transfer/pkg/abstract/model"
	v3credential "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

type YdbStorageParams struct {
	Database           string
	Instance           string
	Tables             []string
	TableColumnsFilter []YdbColumnsFilter
	UseFullPaths       bool

	// auth props
	Token            model.SecretString
	ServiceAccountID string
	UserdataAuth     bool
	SAKeyContent     string
	TokenServiceURL  string
	OAuth2Config     *v3credential.OAuth2Config

	RootCAFiles []string
	TLSEnabled  bool
}
