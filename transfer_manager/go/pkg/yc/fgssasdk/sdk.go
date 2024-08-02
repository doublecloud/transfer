// A temporary package until TM-6301 is resolved.
// We need two SDK instances since we temporarily work on behalf of 2 different
// service accounts.
package fgssasdk

import (
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/yc"
)

var (
	sdk *yc.SDK
)

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
