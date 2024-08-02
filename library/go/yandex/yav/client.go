package yav

import "context"

type Client interface {
	CreateSecret(ctx context.Context, data SecretRequest) (*CreateSecretResponse, error)
	GetSecrets(ctx context.Context, data GetSecretsRequest) (*GetSecretsResponse, error)
	GetSecretVersionsByTokens(ctx context.Context, data GetSecretVersionsByTokensRequest) (*GetSecretVersionsByTokensResponse, error)
	CheckReadAccessRights(ctx context.Context, secretUUID string, userUID uint64) (*CheckReadAccessRightsResponse, error)
	UpdateSecret(ctx context.Context, secretUUID string, r SecretRequest) (*Response, error)
	AddSecretRole(ctx context.Context, secretUUID string, r SecretRoleRequest) (*Response, error)
	DeleteSecretRole(ctx context.Context, secretUUID string, r SecretRoleRequest) (*Response, error)
	GetVersion(ctx context.Context, versionID string) (*GetVersionResponse, error)
	CreateVersion(ctx context.Context, secretID string, data CreateVersionRequest) (*CreateVersionResponse, error)
	CreateVersionFromDiff(ctx context.Context, versionUUID string, r CreateVersionFromDiffRequest) (*CreateVersionFromDiffResponse, error)
	GetTokens(ctx context.Context, secretUUID string, r GetTokensRequest) (*GetTokensResponse, error)
	CreateToken(ctx context.Context, secretUUID string, r CreateTokenRequest) (*CreateTokenResponse, error)
	RevokeToken(ctx context.Context, tokenUUID string) (*Response, error)
	RestoreToken(ctx context.Context, tokenUUID string) (*Response, error)
}
