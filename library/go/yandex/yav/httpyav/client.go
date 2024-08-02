// An HTTP implementation on Yandex.Vault Go client
package httpyav

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"

	"github.com/doublecloud/tross/library/go/httputil/headers"
	"github.com/doublecloud/tross/library/go/yandex/yav"
	"github.com/go-resty/resty/v2"
)

var (
	DefaultHTTPHost = "https://vault-api.passport.yandex.net"
)

var _ yav.Client = new(Client)

type Client struct {
	httpc *resty.Client
}

// NewClient returns new YaV HTTP client
func NewClient(opts ...ClientOpt) (*Client, error) {
	return NewClientWithResty(resty.New(), opts...)
}

// NewClientWithResty returns new YaV HTTP client with custom resty client
func NewClientWithResty(r *resty.Client, opts ...ClientOpt) (*Client, error) {
	c := &Client{
		httpc: r,
	}

	for _, o := range opts {
		if err := o(c); err != nil {
			return nil, err
		}
	}

	if c.httpc.BaseURL == "" {
		c.httpc.SetBaseURL(DefaultHTTPHost)
	}

	vaultURL, err := url.Parse(c.httpc.BaseURL)
	if err != nil {
		return nil, err
	}
	c.httpc.SetHeader("Host", vaultURL.Hostname())

	c.httpc.SetHeader("User-Agent", "YandexVaultGoClient")
	c.httpc.SetHeader(headers.ContentTypeKey, string(headers.TypeApplicationJSON))
	c.httpc.SetAllowGetMethodPayload(true)

	return c, nil
}

// Use is a chain method for inplace client options modification.
// It is useful for TVM YaV authorization.
//
// Example:
//
//	yavc, _ := yav_http.NewClient()
//	resp, err := yavc.Use(
//	    yav_http.WithTVMTickets(
//	        "service_ticket",
//	        "user_ticket",
//	    ),
//	).GetVersion("version_uuid")
func (c *Client) Use(opts ...ClientOpt) *Client {
	for _, o := range opts {
		if err := o(c); err != nil {
			panic(err)
		}
	}
	return c
}

// GetSecrets returns secrets list using given request params
func (c Client) GetSecrets(ctx context.Context, r yav.GetSecretsRequest) (*yav.GetSecretsResponse, error) {
	resp, err := c.httpc.R().
		SetBody(r).
		SetContext(ctx).
		Get("/1/secrets/")

	if err != nil {
		return nil, err
	}

	yr := new(yav.GetSecretsResponse)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

// GetSecretVersionsByTokens returns secret versions by delegation tokens
func (c Client) GetSecretVersionsByTokens(ctx context.Context, r yav.GetSecretVersionsByTokensRequest) (*yav.GetSecretVersionsByTokensResponse, error) {
	resp, err := c.httpc.R().
		SetBody(r).
		SetContext(ctx).
		Post("/1/tokens/")

	if err != nil {
		return nil, err
	}

	yr := new(yav.GetSecretVersionsByTokensResponse)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

func (c Client) CheckReadAccessRights(ctx context.Context, secretUUID string, userUID uint64) (*yav.CheckReadAccessRightsResponse, error) {
	if secretUUID == "" {
		return nil, errors.New("secret_uuid cannot be empty")
	}

	resp, err := c.httpc.R().
		SetContext(ctx).
		SetPathParams(map[string]string{
			"secretUUID": secretUUID,
			"userUID":    strconv.FormatUint(userUID, 10),
		}).
		Get("/1/secrets/{secretUUID}/readers/{userUID}")

	if err != nil {
		return nil, err
	}

	yr := new(yav.CheckReadAccessRightsResponse)
	if err = json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

// CreateSecret creates new secret using given request params
func (c Client) CreateSecret(ctx context.Context, r yav.SecretRequest) (*yav.CreateSecretResponse, error) {
	resp, err := c.httpc.R().
		SetBody(r).
		SetContext(ctx).
		Post("/1/secrets/")

	if err != nil {
		return nil, err
	}

	yr := new(yav.CreateSecretResponse)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

// UpdateSecret updates existing secret metadata using given request params
func (c Client) UpdateSecret(ctx context.Context, secretUUID string, r yav.SecretRequest) (*yav.Response, error) {
	if secretUUID == "" {
		return nil, errors.New("secret_uuid cannot be empty")
	}

	resp, err := c.httpc.R().
		SetBody(r).
		SetContext(ctx).
		SetPathParams(map[string]string{
			"secretUUID": secretUUID,
		}).
		Post("/1/secrets/{secretUUID}/")

	if err != nil {
		return nil, err
	}

	yr := new(yav.Response)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

// AddSecretRole adds new role to existing secret
func (c Client) AddSecretRole(ctx context.Context, secretUUID string, r yav.SecretRoleRequest) (*yav.Response, error) {
	if secretUUID == "" {
		return nil, errors.New("secret_uuid cannot be empty")
	}
	if r.AbcScope != "" && r.AbcRoleID != 0 {
		return nil, errors.New("cannot specify the scope and role for abc service simultaneously")
	}

	resp, err := c.httpc.R().
		SetBody(r).
		SetContext(ctx).
		SetPathParams(map[string]string{
			"secretUUID": secretUUID,
		}).
		Post("/1/secrets/{secretUUID}/roles/")

	if err != nil {
		return nil, err
	}

	yr := new(yav.Response)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

// DeleteSecretRole removes role from existing secret
func (c Client) DeleteSecretRole(ctx context.Context, secretUUID string, r yav.SecretRoleRequest) (*yav.Response, error) {
	if secretUUID == "" {
		return nil, errors.New("secret_uuid cannot be empty")
	}

	resp, err := c.httpc.R().
		SetBody(r).
		SetContext(ctx).
		SetPathParams(map[string]string{
			"secretUUID": secretUUID,
		}).
		Delete("/1/secrets/{secretUUID}/roles/")

	if err != nil {
		return nil, err
	}

	yr := new(yav.Response)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

// GetVersion returns requested secret version.
// You can pass versionUUID as parameter to get exact secret version
// or secretUUID to get most recent (head) secret version.
func (c Client) GetVersion(ctx context.Context, uuid string) (*yav.GetVersionResponse, error) {
	if uuid == "" {
		return nil, errors.New("uuid cannot be empty")
	}

	resp, err := c.httpc.R().
		SetPathParams(map[string]string{
			"uuid": uuid,
		}).
		SetContext(ctx).
		Get("/1/versions/{uuid}/")

	if err != nil {
		return nil, err
	}

	yr := new(yav.GetVersionResponse)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

// CreateVersionFromDiff creates new version of secret version using given diff
func (c Client) CreateVersionFromDiff(ctx context.Context, versionUUID string, r yav.CreateVersionFromDiffRequest) (*yav.CreateVersionFromDiffResponse, error) {
	if versionUUID == "" {
		return nil, errors.New("version_uuid cannot be empty")
	}
	if !strings.HasPrefix(versionUUID, "ver-") {
		return nil, errors.New("version_uuid must have prefix 'ver-'")
	}

	resp, err := c.httpc.R().
		SetPathParams(map[string]string{
			"versionUUID": versionUUID,
		}).
		SetBody(r).
		SetContext(ctx).
		Post("/1/versions/{versionUUID}")

	if err != nil {
		return nil, err
	}

	yr := new(yav.CreateVersionFromDiffResponse)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

// CreateVersion creates new version of secret using given request params
func (c Client) CreateVersion(ctx context.Context, secretUUID string, r yav.CreateVersionRequest) (*yav.CreateVersionResponse, error) {
	if secretUUID == "" {
		return nil, errors.New("secret_uuid cannot be empty")
	}

	resp, err := c.httpc.R().
		SetPathParams(map[string]string{
			"secretUUID": secretUUID,
		}).
		SetBody(r).
		SetContext(ctx).
		Post("/1/secrets/{secretUUID}/versions/")

	if err != nil {
		return nil, err
	}

	yr := new(yav.CreateVersionResponse)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

// GetTokens returns delegation tokens for secret.
func (c Client) GetTokens(ctx context.Context, secretUUID string, r yav.GetTokensRequest) (*yav.GetTokensResponse, error) {
	if secretUUID == "" {
		return nil, errors.New("secret_uuid cannot be empty")
	}

	resp, err := c.httpc.R().
		SetBody(r).
		SetPathParams(map[string]string{
			"secretUUID": secretUUID,
		}).
		SetContext(ctx).
		Get("/1/secrets/{secretUUID}/tokens")

	if err != nil {
		return nil, err
	}

	yr := new(yav.GetTokensResponse)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

// CreateToken creates new delegation token for secret using given request params
func (c Client) CreateToken(ctx context.Context, secretUUID string, r yav.CreateTokenRequest) (*yav.CreateTokenResponse, error) {
	if secretUUID == "" {
		return nil, errors.New("secret_uuid cannot be empty")
	}

	resp, err := c.httpc.R().
		SetBody(r).
		SetPathParams(map[string]string{
			"secretUUID": secretUUID,
		}).
		SetContext(ctx).
		Post("/1/secrets/{secretUUID}/tokens")

	if err != nil {
		return nil, err
	}

	yr := new(yav.CreateTokenResponse)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

func (c Client) RevokeToken(ctx context.Context, tokenUUID string) (*yav.Response, error) {
	if tokenUUID == "" {
		return nil, errors.New("token_uuid cannot be empty")
	}

	resp, err := c.httpc.R().
		SetContext(ctx).
		SetPathParams(map[string]string{
			"tokenUUID": tokenUUID,
		}).
		Post("/1/tokens/{tokenUUID}/revoke/")

	if err != nil {
		return nil, err
	}

	yr := new(yav.Response)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}

func (c Client) RestoreToken(ctx context.Context, tokenUUID string) (*yav.Response, error) {
	if tokenUUID == "" {
		return nil, errors.New("token_uuid cannot be empty")
	}

	resp, err := c.httpc.R().
		SetContext(ctx).
		SetPathParams(map[string]string{
			"tokenUUID": tokenUUID,
		}).
		Post("/1/tokens/{tokenUUID}/restore/")

	if err != nil {
		return nil, err
	}

	yr := new(yav.Response)
	if err := json.Unmarshal(resp.Body(), yr); err != nil {
		return nil, err
	}

	return yr, nil
}
