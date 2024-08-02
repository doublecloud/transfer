package config

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
	"time"

	"github.com/doublecloud/tross/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	iampb "github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/yc"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// go-sumtype:decl CloudCreds
type CloudCreds interface {
	TypeTagged
	isCloudCreds()
}

type PassportOauthToken struct {
	Token Secret `mapstructure:"token"`
}

type ServiceAccountKey struct {
	ID               string `mapstructure:"id" json:"id"`
	ServiceAccountID string `mapstructure:"service_account_id" json:"service_account_id"`
	KeyAlgorithm     string `mapstructure:"key_algorithm" json:"key_algorithm"`
	PublicKey        string `mapstructure:"public_key" json:"public_key"`
	PrivateKey       Secret `mapstructure:"private_key" json:"private_key"`
}

type InstanceMetadata struct{}

type ServiceAccountKeyFile struct {
	FileName string `mapstructure:"file_name"`
}

type ConstantIAMToken struct {
	Token Secret `mapstructure:"token"`
}

type YcProfile struct {
	ProfileName string `mapstructure:"profile_name" yaml:"profile_name"`
}

func (t *YcProfile) YandexCloudAPICredentials() {}
func (t *YcProfile) IAMToken(ctx context.Context) (*iampb.CreateIamTokenResponse, error) {
	command := exec.Command("yc", "iam", "create-token", "--profile", t.ProfileName)
	token, err := command.Output()
	if err != nil {
		return nil, xerrors.Errorf("unable to get cli output: %w", err)
	}

	iamToken := strings.TrimSuffix(string(token), "\n")
	return &iampb.CreateIamTokenResponse{
		IamToken:  iamToken,
		ExpiresAt: &timestamppb.Timestamp{Seconds: time.Now().Add(time.Hour * 12).Unix()},
	}, nil
}

func (t *YcProfile) APIEndpoint() (string, error) {
	command := exec.Command("yc", "config", "get", "endpoint", "--profile", t.ProfileName)
	endpoint, err := command.Output()
	if err != nil {
		return "", xerrors.Errorf("unable to get cli output: %w", err)
	}
	return string(endpoint), nil
}

type YcpProfile struct {
	ProfileName string `mapstructure:"profile_name" yaml:"profile_name"`
}

func (t *YcpProfile) YandexCloudAPICredentials() {}
func (t *YcpProfile) IAMToken(ctx context.Context) (*iampb.CreateIamTokenResponse, error) {
	command := exec.Command("ycp", "iam", "create-token", "--profile", t.ProfileName)
	token, err := command.Output()
	if err != nil {
		return nil, xerrors.Errorf("unable to get cli output: %w", err)
	}

	iamToken := strings.TrimSuffix(string(token), "\n")
	return &iampb.CreateIamTokenResponse{
		IamToken:  iamToken,
		ExpiresAt: &timestamppb.Timestamp{Seconds: time.Now().Add(time.Hour * 12).Unix()},
	}, nil
}

func (*PassportOauthToken) isCloudCreds() {}
func (*PassportOauthToken) IsTypeTagged() {}

func (*ServiceAccountKey) isCloudCreds() {}
func (*ServiceAccountKey) IsTypeTagged() {}

func (*InstanceMetadata) isCloudCreds() {}
func (*InstanceMetadata) IsTypeTagged() {}

func (*ServiceAccountKeyFile) isCloudCreds() {}
func (*ServiceAccountKeyFile) IsTypeTagged() {}

func (*ConstantIAMToken) isCloudCreds() {}
func (*ConstantIAMToken) IsTypeTagged() {}

func (*YcProfile) isCloudCreds() {}
func (*YcProfile) IsTypeTagged() {}

func (*YcpProfile) isCloudCreds() {}
func (*YcpProfile) IsTypeTagged() {}

func (t *ConstantIAMToken) YandexCloudAPICredentials() {}
func (t *ConstantIAMToken) IAMToken(ctx context.Context) (*iampb.CreateIamTokenResponse, error) {
	return &iampb.CreateIamTokenResponse{
		IamToken:  string(t.Token),
		ExpiresAt: &timestamppb.Timestamp{Seconds: time.Now().Add(time.Hour * 12).Unix()},
	}, nil
}

func init() {
	RegisterTypeTagged((*CloudCreds)(nil), (*PassportOauthToken)(nil), "passport_oauth_token", nil)
	RegisterTypeTagged((*CloudCreds)(nil), (*ServiceAccountKey)(nil), "service_account_key", nil)
	RegisterTypeTagged((*CloudCreds)(nil), (*InstanceMetadata)(nil), "instance_metadata", nil)
	RegisterTypeTagged((*CloudCreds)(nil), (*YcProfile)(nil), "yc_profile", nil)
	RegisterTypeTagged((*CloudCreds)(nil), (*YcpProfile)(nil), "ycp_profile", nil)
	RegisterTypeTagged((*CloudCreds)(nil), (*ServiceAccountKeyFile)(nil), "service_account_key_file", nil)
	RegisterTypeTagged((*CloudCreds)(nil), (*ConstantIAMToken)(nil), "iam_token", nil)
}

func ToYCCredentials(creds CloudCreds) (yc.Credentials, error) {
	var ycCreds yc.Credentials
	switch configuredCreds := creds.(type) {
	case *ServiceAccountKeyFile:
		fileName := configuredCreds.FileName
		fileContent, err := ioutil.ReadFile(fileName)
		if err != nil {
			return nil, xerrors.Errorf(`Can't read file "%s" for credentials`, fileName)
		}
		creds := new(ServiceAccountKey)
		if err := json.Unmarshal([]byte(fileContent), &creds); err != nil {
			return nil, xerrors.Errorf(`Can't unmarshal file "%s" for credentials`, fileName)
		}
		iamKey := iam.Key{
			Id:           creds.ID,
			Subject:      &iam.Key_ServiceAccountId{ServiceAccountId: creds.ServiceAccountID},
			Description:  fmt.Sprintf("key for service account: %s", creds.ServiceAccountID),
			KeyAlgorithm: iam.Key_Algorithm(iam.Key_Algorithm_value[creds.KeyAlgorithm]),
			PublicKey:    creds.PublicKey,
		}
		ycCreds, err = yc.ServiceAccountKey(&iamKey, string(creds.PrivateKey))
		if err != nil {
			return nil, xerrors.Errorf("Cannot create service account key: %w", err)
		}
	case *YcpProfile:
		ycCreds = configuredCreds
	case *YcProfile:
		ycCreds = configuredCreds
	case *InstanceMetadata:
		ycCreds = yc.InstanceServiceAccount()
	case *PassportOauthToken:
		ycCreds = yc.OAuthToken(string(configuredCreds.Token))
	case *ServiceAccountKey:
		iamKey := iam.Key{
			Id:           configuredCreds.ID,
			Subject:      &iam.Key_ServiceAccountId{ServiceAccountId: configuredCreds.ServiceAccountID},
			Description:  fmt.Sprintf("key for service account: %s", configuredCreds.ServiceAccountID),
			KeyAlgorithm: iam.Key_Algorithm(iam.Key_Algorithm_value[configuredCreds.KeyAlgorithm]),
			PublicKey:    configuredCreds.PublicKey,
		}
		var err error
		ycCreds, err = yc.ServiceAccountKey(&iamKey, string(configuredCreds.PrivateKey))
		if err != nil {
			return nil, xerrors.Errorf("Cannot create service account key: %w", err)
		}
	case *ConstantIAMToken:
		ycCreds = configuredCreds
	}
	if ycCreds == nil {
		return nil, xerrors.Errorf("Unknown credentials type: %T", creds)
	}
	return ycCreds, nil
}
