package config

import (
	"context"

	"cloud.google.com/go/compute/metadata"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/doublecloud/tross/library/go/core/xerrors"
)

type tokenFetcher struct{}

func (t tokenFetcher) FetchToken(context.Context) ([]byte, error) {
	token, err := metadata.Get("instance/service-accounts/default/identity?format=standard&audience=gcp")
	if err != nil {
		return nil, xerrors.Errorf("unable to fetch gcp token: %w", err)
	}

	return []byte(token), nil
}

func NewAWSCreds(role string) *credentials.Credentials {
	if role == "" { // no role to create creds
		return nil
	}
	if !metadata.OnGCE() {
		return nil
	}
	provider := stscreds.NewWebIdentityRoleProviderWithOptions(
		sts.New(session.Must(session.NewSession())),
		role,
		"",
		&tokenFetcher{},
	)

	return credentials.NewCredentials(provider)
}
