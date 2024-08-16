package conn

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/xtls"
)

func NewTLS(config ConnParams) (*tls.Config, error) {
	if !config.SSLEnabled() {
		return nil, nil
	}
	if config.PemFileContent() != "" {
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM([]byte(config.PemFileContent())) {
			return nil, xerrors.Errorf("credentials: failed to append certificates")
		}
		return &tls.Config{
			RootCAs: cp,
		}, nil
	}
	tlsConfig, err := xtls.FromPath(config.RootCertPaths())
	if err != nil {
		return nil, xerrors.Errorf("unable to get dataplane tls: %w", err)
	}
	return tlsConfig, nil
}
