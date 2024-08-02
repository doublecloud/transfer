package httpyav_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/tross/library/go/yandex/yav"
	"github.com/doublecloud/tross/library/go/yandex/yav/httpyav"
	"github.com/stretchr/testify/assert"
)

func TestClient_GetSecrets(t *testing.T) {
	testCases := []struct {
		name           string
		bootstrap      func() (*httptest.Server, *httpyav.Client)
		params         yav.GetSecretsRequest
		expectResponse *yav.GetSecretsResponse
		expectError    errChecker
	}{
		{
			"net_error",
			func() (*httptest.Server, *httpyav.Client) {
				var ts *httptest.Server
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ts.CloseClientConnections()
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			yav.GetSecretsRequest{
				Query:     "ololo",
				QueryType: yav.SecretQueryTypeExact,
			},
			nil,
			isNetError(),
		},
		{
			"error_code",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"status": "error", "code": "invalid_oauth_token_error"}`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			yav.GetSecretsRequest{
				Query:     "ololo",
				QueryType: yav.SecretQueryTypeExact,
			},
			&yav.GetSecretsResponse{
				Response: yav.Response{
					Status: "error",
					Code:   "invalid_oauth_token_error",
				},
			},
			nil,
		},
		{
			"success",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`
						{
							"status": "success",
							"page": 1,
							"page_size": 1,
							"secrets": [
								{
									"created_at": 1564828284.0,
									"created_by": 42,
									"creator_login": "ololoid",
									"name": "ololo",
									"updated_at": 1564828284.0,
									"updated_by": 42,
									"uuid": "sec-000001400089",
									"versions_count": 1
								}
							]
						}
					`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			yav.GetSecretsRequest{
				Query:     "ololo",
				QueryType: yav.SecretQueryTypeExact,
			},
			&yav.GetSecretsResponse{
				Response: yav.Response{
					Status: "success",
				},
				Page:     1,
				PageSize: 1,
				Secrets: []yav.Secret{
					{
						CreatedAt:     yav.Timestamp{Time: time.Unix(1564828284, 0)},
						CreatedBy:     42,
						CreatorLogin:  "ololoid",
						Name:          "ololo",
						UpdatedAt:     yav.Timestamp{Time: time.Unix(1564828284, 0)},
						UpdatedBy:     42,
						SecretUUID:    "sec-000001400089",
						VersionsCount: 1,
					},
				},
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, c := tc.bootstrap()
			defer ts.Close()

			ctx := context.Background()
			resp, err := c.GetSecrets(ctx, tc.params)

			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.True(t, tc.expectError(err), "unexpected error: %+v", err)
			}

			assert.Equal(t, tc.expectResponse, resp)
		})
	}
}

func TestClient_CreateSecret(t *testing.T) {
	testCases := []struct {
		name           string
		bootstrap      func() (*httptest.Server, *httpyav.Client)
		params         yav.SecretRequest
		expectResponse *yav.CreateSecretResponse
		expectError    errChecker
	}{
		{
			"net_error",
			func() (*httptest.Server, *httpyav.Client) {
				var ts *httptest.Server
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ts.CloseClientConnections()
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			yav.SecretRequest{
				Name:    "ololo",
				Comment: "trololo",
				Tags:    []string{"looken", "tooken"},
			},
			nil,
			isNetError(),
		},
		{
			"error_code",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"status": "error", "code": "invalid_oauth_token_error"}`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			yav.SecretRequest{
				Name:    "ololo",
				Comment: "trololo",
				Tags:    []string{"looken", "tooken"},
			},
			&yav.CreateSecretResponse{
				Response: yav.Response{
					Status: "error",
					Code:   "invalid_oauth_token_error",
				},
			},
			nil,
		},
		{
			"success",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`
						{
							"status": "success",
							"uuid": "sec-000001400089"
						}
					`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			yav.SecretRequest{
				Name:    "ololo",
				Comment: "trololo",
				Tags:    []string{"looken", "tooken"},
			},
			&yav.CreateSecretResponse{
				Response: yav.Response{
					Status: "success",
				},
				SecretUUID: "sec-000001400089",
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, c := tc.bootstrap()
			defer ts.Close()

			ctx := context.Background()
			resp, err := c.CreateSecret(ctx, tc.params)

			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.True(t, tc.expectError(err), "unexpected error: %+v", err)
			}

			assert.Equal(t, tc.expectResponse, resp)
		})
	}
}

func TestClient_UpdateSecret(t *testing.T) {
	testCases := []struct {
		name           string
		bootstrap      func() (*httptest.Server, *httpyav.Client)
		secretUUID     string
		params         yav.SecretRequest
		expectResponse *yav.Response
		expectError    errChecker
	}{
		{
			"empty_secret_uuid",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"",
			yav.SecretRequest{
				Name:    "ololo",
				Comment: "trololo",
				State:   yav.SecretStateHidden,
				Tags:    []string{"looken", "tooken"},
			},
			nil,
			isError(errors.New("secret_uuid cannot be empty")),
		},
		{
			"net_error",
			func() (*httptest.Server, *httpyav.Client) {
				var ts *httptest.Server
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ts.CloseClientConnections()
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.SecretRequest{
				Name:    "ololo",
				Comment: "trololo",
				State:   yav.SecretStateHidden,
				Tags:    []string{"looken", "tooken"},
			},
			nil,
			isNetError(),
		},
		{
			"error_code",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"status": "error", "code": "invalid_oauth_token_error"}`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.SecretRequest{
				Name:    "ololo",
				Comment: "trololo",
				State:   yav.SecretStateHidden,
				Tags:    []string{"looken", "tooken"},
			},
			&yav.Response{
				Status: "error",
				Code:   "invalid_oauth_token_error",
			},
			nil,
		},
		{
			"success",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`
						{
							"status": "success"
						}
					`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.SecretRequest{
				Name:    "ololo",
				Comment: "trololo",
				State:   yav.SecretStateHidden,
				Tags:    []string{"looken", "tooken"},
			},
			&yav.Response{
				Status: "success",
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, c := tc.bootstrap()
			defer ts.Close()

			ctx := context.Background()
			resp, err := c.UpdateSecret(ctx, tc.secretUUID, tc.params)

			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.True(t, tc.expectError(err), "unexpected error: %+v", err)
			}

			assert.Equal(t, tc.expectResponse, resp)
		})
	}
}

func TestClient_AddSecretRole(t *testing.T) {
	testCases := []struct {
		name           string
		bootstrap      func() (*httptest.Server, *httpyav.Client)
		secretUUID     string
		params         yav.SecretRoleRequest
		expectResponse *yav.Response
		expectError    errChecker
	}{
		{
			"empty_secret_uuid",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"",
			yav.SecretRoleRequest{
				Login: "pg",
			},
			nil,
			isError(errors.New("secret_uuid cannot be empty")),
		},
		{
			"net_error",
			func() (*httptest.Server, *httpyav.Client) {
				var ts *httptest.Server
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ts.CloseClientConnections()
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.SecretRoleRequest{
				Login: "pg",
			},
			nil,
			isNetError(),
		},
		{
			"error_code",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"status": "error", "code": "invalid_oauth_token_error"}`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.SecretRoleRequest{
				Login: "pg",
			},
			&yav.Response{
				Status: "error",
				Code:   "invalid_oauth_token_error",
			},
			nil,
		},
		{
			"success",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`
						{
							"status": "success"
						}
					`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.SecretRoleRequest{
				Login: "pg",
			},
			&yav.Response{
				Status: "success",
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, c := tc.bootstrap()
			defer ts.Close()

			ctx := context.Background()
			resp, err := c.AddSecretRole(ctx, tc.secretUUID, tc.params)

			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.True(t, tc.expectError(err), "unexpected error: %+v", err)
			}

			assert.Equal(t, tc.expectResponse, resp)
		})
	}
}

func TestClient_DeleteSecretRole(t *testing.T) {
	testCases := []struct {
		name           string
		bootstrap      func() (*httptest.Server, *httpyav.Client)
		secretUUID     string
		params         yav.SecretRoleRequest
		expectResponse *yav.Response
		expectError    errChecker
	}{
		{
			"empty_secret_uuid",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"",
			yav.SecretRoleRequest{
				Login: "pg",
			},
			nil,
			isError(errors.New("secret_uuid cannot be empty")),
		},
		{
			"net_error",
			func() (*httptest.Server, *httpyav.Client) {
				var ts *httptest.Server
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ts.CloseClientConnections()
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.SecretRoleRequest{
				Login: "pg",
			},
			nil,
			isNetError(),
		},
		{
			"error_code",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"status": "error", "code": "invalid_oauth_token_error"}`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.SecretRoleRequest{
				Login: "pg",
			},
			&yav.Response{
				Status: "error",
				Code:   "invalid_oauth_token_error",
			},
			nil,
		},
		{
			"success",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`
						{
							"status": "success"
						}
					`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.SecretRoleRequest{
				Login: "pg",
			},
			&yav.Response{
				Status: "success",
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, c := tc.bootstrap()
			defer ts.Close()

			ctx := context.Background()
			resp, err := c.DeleteSecretRole(ctx, tc.secretUUID, tc.params)

			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.True(t, tc.expectError(err), "unexpected error: %+v", err)
			}

			assert.Equal(t, tc.expectResponse, resp)
		})
	}
}

func TestClient_GetVersion(t *testing.T) {
	testCases := []struct {
		name           string
		bootstrap      func() (*httptest.Server, *httpyav.Client)
		versionUUID    string
		expectResponse *yav.GetVersionResponse
		expectError    errChecker
	}{
		{
			"empty_secret_uuid",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"",
			nil,
			isError(errors.New("uuid cannot be empty")),
		},
		{
			"net_error",
			func() (*httptest.Server, *httpyav.Client) {
				var ts *httptest.Server
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ts.CloseClientConnections()
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"ver-000001400089",
			nil,
			isNetError(),
		},
		{
			"error_code",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"status": "error", "code": "invalid_oauth_token_error"}`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"ver-000001400089",
			&yav.GetVersionResponse{
				Response: yav.Response{
					Status: "error",
					Code:   "invalid_oauth_token_error",
				},
			},
			nil,
		},
		{
			"success",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`
						{
							"status": "success",
							"version": {
								"created_at": 1564828284,
								"created_by": 42,
								"creator_login": "pg",
								"secret_name": "ololo",
								"secret_uuid": "sec-000001400089",
								"value": [
									{
										"key": "looken",
										"value": "tooken"
									}
								],
								"version": "ver-000001400089"
							}
						}
					`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"ver-000001400089",
			&yav.GetVersionResponse{
				Response: yav.Response{
					Status: "success",
				},
				Version: yav.Version{
					CreatedAt:    yav.Timestamp{Time: time.Unix(1564828284, 0)},
					CreatedBy:    42,
					CreatorLogin: "pg",
					SecretName:   "ololo",
					SecretUUID:   "sec-000001400089",
					Values: []yav.Value{
						{Key: "looken", Value: "tooken"},
					},
					VersionUUID: "ver-000001400089",
				},
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, c := tc.bootstrap()
			defer ts.Close()

			ctx := context.Background()
			resp, err := c.GetVersion(ctx, tc.versionUUID)

			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.True(t, tc.expectError(err), "unexpected error: %+v", err)
			}

			assert.Equal(t, tc.expectResponse, resp)
		})
	}
}

func TestClient_CheckReadAccessRights(t *testing.T) {
	testCases := []struct {
		name           string
		bootstrap      func() (*httptest.Server, *httpyav.Client)
		secretUUID     string
		userUID        uint64
		expectResponse *yav.CheckReadAccessRightsResponse
		expectError    errChecker
	}{
		{
			"empty_secret_uuid",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"",
			42,
			nil,
			isError(errors.New("secret_uuid cannot be empty")),
		},
		{
			"net_error",
			func() (*httptest.Server, *httpyav.Client) {
				var ts *httptest.Server
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ts.CloseClientConnections()
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			42,
			nil,
			isNetError(),
		},
		{
			"error_code",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"status": "error", "code": "invalid_oauth_token_error"}`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			42,
			&yav.CheckReadAccessRightsResponse{
				Response: yav.Response{
					Status: "error",
					Code:   "invalid_oauth_token_error",
				},
			},
			nil,
		},
		{
			"success",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`
						{
							"access": "allowed",
							"name": "secret_1",
							"secret_uuid": "sec-000001400089",
							"status": "ok",
							"user": {
								"login": "answer",
								"uid": 42
							}
						}
					`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			42,
			&yav.CheckReadAccessRightsResponse{
				Response: yav.Response{
					Status: "ok",
				},
				Access:     "allowed",
				Name:       "secret_1",
				SecretUUID: "sec-000001400089",
				User: yav.User{
					Login: "answer",
					UID:   42,
				},
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, c := tc.bootstrap()
			defer ts.Close()

			ctx := context.Background()
			resp, err := c.CheckReadAccessRights(ctx, tc.secretUUID, tc.userUID)

			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.True(t, tc.expectError(err), "unexpected error: %+v", err)
			}

			assert.Equal(t, tc.expectResponse, resp)
		})
	}
}

func TestClient_CreateVersion(t *testing.T) {
	testCases := []struct {
		name           string
		bootstrap      func() (*httptest.Server, *httpyav.Client)
		secretUUID     string
		params         yav.CreateVersionRequest
		expectResponse *yav.CreateVersionResponse
		expectError    errChecker
	}{
		{
			"empty_secret_uuid",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"",
			yav.CreateVersionRequest{
				Comment: "trololo",
				Values: []yav.Value{
					{Key: "looken", Value: "tooken"},
				},
			},
			nil,
			isError(errors.New("secret_uuid cannot be empty")),
		},
		{
			"net_error",
			func() (*httptest.Server, *httpyav.Client) {
				var ts *httptest.Server
				ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ts.CloseClientConnections()
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.CreateVersionRequest{
				Comment: "trololo",
				Values: []yav.Value{
					{Key: "looken", Value: "tooken"},
				},
			},
			nil,
			isNetError(),
		},
		{
			"error_code",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`{"status": "error", "code": "invalid_oauth_token_error"}`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.CreateVersionRequest{
				Comment: "trololo",
				Values: []yav.Value{
					{Key: "looken", Value: "tooken"},
				},
			},
			&yav.CreateVersionResponse{
				Response: yav.Response{
					Status: "error",
					Code:   "invalid_oauth_token_error",
				},
			},
			nil,
		},
		{
			"success",
			func() (*httptest.Server, *httpyav.Client) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte(`
						{
							"status": "success",
							"secret_version": "ver-000001400089"
						}
					`))
				}))

				c, _ := httpyav.NewClient(
					httpyav.WithOAuthToken("looken-tooken"),
					httpyav.WithHTTPHost(ts.URL),
				)
				return ts, c
			},
			"sec-000001400089",
			yav.CreateVersionRequest{
				Comment: "trololo",
				Values: []yav.Value{
					{Key: "looken", Value: "tooken"},
				},
			},
			&yav.CreateVersionResponse{
				Response: yav.Response{
					Status: "success",
				},
				VersionUUID: "ver-000001400089",
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, c := tc.bootstrap()
			defer ts.Close()

			ctx := context.Background()
			resp, err := c.CreateVersion(ctx, tc.secretUUID, tc.params)

			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.True(t, tc.expectError(err), "unexpected error: %+v", err)
			}

			assert.Equal(t, tc.expectResponse, resp)
		})
	}
}

type errChecker func(error) bool

func isNetError() func(error) bool {
	return func(err error) bool {
		var netErr net.Error
		return errors.As(err, &netErr)
	}
}

func isError(target error) func(error) bool {
	return func(err error) bool {
		return errors.Is(err, target) || strings.Contains(err.Error(), target.Error())
	}
}
