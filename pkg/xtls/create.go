package xtls

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
)

func Pool(rootCACertPaths []string) (*x509.CertPool, error) {
	cp, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	for _, certPath := range rootCACertPaths {
		caCertificatePEM, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, xerrors.Errorf("Cannot read file %s: %w", certPath, err)
		}
		if !cp.AppendCertsFromPEM(caCertificatePEM) {
			return nil, xerrors.Errorf("credentials: failed to append certificates")
		}
	}
	return cp, nil
}

func FromPath(rootCACertPaths []string) (*tls.Config, error) {
	cp, err := Pool(rootCACertPaths)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		RootCAs: cp,
	}, nil
}
