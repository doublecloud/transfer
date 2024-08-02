package ycsdk

import (
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/config"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/yc"
)

var (
	sdk *yc.SDK
)

func InitializeWithConfig(cfg yc.Config) (err error) {
	sdk, err = yc.Build(cfg)
	return err
}

func InitializeWithCreds(endpoint string, credentials yc.Credentials) (err error) {
	sdk, err = yc.Build(yc.MakeConfig(credentials, endpoint))
	return err
}

// Before using Instance initialize the package using either Initialize or InitializeWithCreds.
func Instance() (*yc.SDK, error) {
	if sdk != nil {
		return sdk, nil
	}
	return nil, xerrors.New("YC SDK has not been initialized")
}

func InitializeWithCloudCreds(endpoint string, creds config.CloudCreds) error { // for tests
	ycCreds, err := config.ToYCCredentials(creds)
	if err != nil {
		return xerrors.Errorf("Cannot create YC credentials: %w", err)
	}

	if err := InitializeWithCreds(endpoint, ycCreds); err != nil {
		return err
	}
	return nil
}
