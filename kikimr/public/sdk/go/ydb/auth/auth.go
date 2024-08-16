package auth

import (
	"context"
	"fmt"
	"os"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/auth/iam"
)

// FromEnviron returns default credentials from environ
func FromEnviron(ctx context.Context) (ydb.Credentials, error) {
	if serviceAccountKeyFile, ok := os.LookupEnv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"); ok {
		c, err := iam.NewClient(
			iam.WithServiceFile(serviceAccountKeyFile),
			iam.WithDefaultEndpoint(),
			iam.WithSystemCertPool(),
			iam.WithSourceInfo("auth.FromEnviron(Env['YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS'])"),
		)
		if err != nil {
			return nil, fmt.Errorf("configure credentials error: %w", err)
		}
		return c, nil
	}
	if os.Getenv("YDB_ANONYMOUS_CREDENTIALS") == "1" {
		return ydb.NewAnonymousCredentials("auth.FromEnviron(Env['YDB_ANONYMOUS_CREDENTIALS'])"), nil
	}
	if os.Getenv("YDB_METADATA_CREDENTIALS") == "1" {
		return iam.InstanceServiceAccount(
			ydb.WithCredentialsSourceInfo(ctx, "auth.FromEnviron(Env['YDB_METADATA_CREDENTIALS'])"),
		), nil
	}
	if accessToken, ok := os.LookupEnv("YDB_ACCESS_TOKEN_CREDENTIALS"); ok {
		return ydb.NewAuthTokenCredentials(accessToken, "auth.FromEnviron(Env['YDB_ACCESS_TOKEN_CREDENTIALS'])"), nil
	}
	return iam.InstanceServiceAccount(
		ydb.WithCredentialsSourceInfo(ctx, "auth.FromEnviron('otherwise - no known environment variables')"),
	), nil
}
