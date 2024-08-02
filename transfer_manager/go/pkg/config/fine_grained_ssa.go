package config

import (
	"crypto/x509"
	"encoding/pem"
	"os"

	"github.com/doublecloud/tross/library/go/core/xerrors"
)

type FineGrainedSSA struct {
	// Compute CA certificate from https://cloud.yandex.ru/ru/docs/compute/concepts/vm-metadata#signed-identity-documents
	CertificatePath           string                   `mapstructure:"certificate_path"`
	ServiceID                 string                   `mapstructure:"service_id"`
	MicroserviceID            string                   `mapstructure:"microservice_id"`
	ControlPlaneSAKeySecretID string                   `mapstructure:"control_plane_sa_key_secret_id"`
	DataPlaneServiceAccounts  DataPlaneServiceAccounts `mapstructure:"data_plane_service_accounts"`
}

type DataPlaneServiceAccounts struct {
	InstanceGroupSAID string `mapstructure:"instance_group_sa_id"`
	InstanceSAID      string `mapstructure:"instance_sa_id"`
}

func (c *FineGrainedSSA) Certificate() (*x509.Certificate, error) {
	return ParseCertificateFromFile(c.CertificatePath)
}

func ParseCertificateFromFile(certPath string) (*x509.Certificate, error) {
	PEMCertificate, err := os.ReadFile(certPath)
	if err != nil {
		return nil, xerrors.Errorf("Cannot read file: %w", err)
	}
	return ParseCertificate(string(PEMCertificate))
}

func ParseCertificate(PEMCertificate string) (*x509.Certificate, error) {
	decodedCertificate, _ := pem.Decode([]byte(PEMCertificate))
	if decodedCertificate == nil {
		return nil, xerrors.New("Cannot decode pem-encoded certificate")
	}
	if decodedCertificate.Type != "CERTIFICATE" {
		return nil, xerrors.Errorf("Pem type isn't certificate: %s", decodedCertificate.Type)
	}
	certificate, err := x509.ParseCertificate(decodedCertificate.Bytes)
	if err != nil {
		return nil, xerrors.Errorf("Cannot parse certificate: %w", err)
	}
	return certificate, nil
}
