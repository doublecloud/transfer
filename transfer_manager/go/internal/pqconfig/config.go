package pqconfig

import (
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/xtls"
	yccreds "github.com/doublecloud/tross/transfer_manager/go/pkg/yc/credentials"
)

func NewLogPQConfig(lbConfig *config.LogLogbroker) (opts *persqueue.WriterOptions, err error) {
	creds, err := ResolveCreds(lbConfig.Creds)
	if err != nil {
		return nil, xerrors.Errorf("Cannot init persqueue writer config without credentials client: %w", err)
	}

	tlsConfig, err := ResolveTLS(lbConfig.TLSMode)
	if err != nil {
		return nil, xerrors.Errorf("Cannot initialize TLS config: %w", err)
	}

	sourceID := lbConfig.SourceID
	if sourceID == "" {
		host, _ := os.Hostname()
		sourceID = fmt.Sprintf("%s/%d", host, time.Now().Second())

	}

	return &persqueue.WriterOptions{
		Topic:          lbConfig.Topic,
		Endpoint:       lbConfig.Endpoint,
		Database:       lbConfig.Database,
		Credentials:    creds,
		TLSConfig:      tlsConfig,
		SourceID:       []byte(sourceID),
		RetryOnFailure: true,
		MaxMemory:      50 * 1024 * 1024,
	}, nil
}

func ResolveCreds(cfg config.LogbrokerCredentials) (ydb.Credentials, error) {
	var creds ydb.Credentials
	switch specificCreds := cfg.(type) {
	case *config.UseCloudCreds:
		return yccreds.NewIamCreds(logger.Log)
	case *config.LogbrokerOAuthToken:
		creds = ydb.AuthTokenCredentials{AuthToken: string(specificCreds.Token)}
	}
	return creds, nil
}

func ResolveTLS(cfg config.TLSMode) (*tls.Config, error) {
	var tlsConfig *tls.Config
	switch tlsMode := cfg.(type) {
	case *config.TLSModeDisabled:
	case *config.TLSModeEnabled:
		return xtls.FromPath(tlsMode.CACertificatePaths)
	}
	return tlsConfig, nil
}
