package httpyav

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/doublecloud/tross/library/go/yandex/yav"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRSASignRequest(t *testing.T) {
	testCases := []struct {
		request *http.Request
		login   string
		privKey *rsa.PrivateKey
	}{
		{
			request: func() *http.Request {
				body := yav.GetSecretsRequest{Tags: []string{"tag1", "tag2"}, Query: "тестовый секрет"}
				b, err := json.Marshal(body)
				require.NoError(t, err)

				req, err := http.NewRequest("GET", "http://vault-api.passport.yandex.net/1/secrets/", bytes.NewReader(b))
				require.NoError(t, err)

				return req
			}(),
			login: "ppodolsky",
			privKey: func() *rsa.PrivateKey {
				privPem, _ := pem.Decode([]byte(testPrivateRSAKey))
				pk, err := x509.ParsePKCS1PrivateKey(privPem.Bytes)
				require.NoError(t, err)
				return pk
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			err := rsaKeySignRequest(tc.request, tc.login, tc.privKey)
			assert.NoError(t, err)

			assert.Contains(t, tc.request.Header, "X-Ya-Rsa-Signature")
			assert.Contains(t, tc.request.Header, "X-Ya-Rsa-Login")
			assert.Contains(t, tc.request.Header, "X-Ya-Rsa-Timestamp")

			assert.Equal(t, tc.request.Header.Get("X-Ya-Rsa-Login"), tc.login)

			// compare signatures
			timestamp, err := strconv.ParseInt(tc.request.Header.Get("X-Ya-Rsa-Timestamp"), 10, 64)
			assert.NoError(t, err)

			serialized, err := serializeRequestV3(
				tc.request,
				time.Unix(timestamp, 0),
				tc.login,
			)
			assert.NoError(t, err)

			sig, err := computeRSASignature(serialized, tc.privKey)
			assert.NoError(t, err)

			assert.Equal(t, sig, tc.request.Header.Get("X-Ya-Rsa-Signature"))
		})
	}
}

func TestSerializeRequest(t *testing.T) {
	testCases := []struct {
		name         string
		request      *http.Request
		ts           time.Time
		login        string
		expectResult string
	}{
		{
			name: "no_payload",
			request: func() *http.Request {
				req, err := http.NewRequest("GET", "http://vault-api.passport.yandex.net/1/versions/ver-kek/", nil)
				require.NoError(t, err)
				req.GetBody = func() (io.ReadCloser, error) {
					// https://github.com/doublecloud/tross/arcadia/vendor/github.com/go-resty/resty/v2/middleware.go?rev=r8841914#L214-219
					return nil, nil
				}
				return req
			}(),
			ts:           time.Unix(1565681543, 0),
			login:        "gzuykov",
			expectResult: "GET\nhttp://vault-api.passport.yandex.net/1/versions/ver-kek/?\n\n1565681543\ngzuykov\n",
		},
		{
			name: "with_payload",
			request: func() *http.Request {
				body := `{"tags":["tag1","tag2"], "query": "тестовый секрет"}`
				req, err := http.NewRequest("GET", "http://vault-api.passport.yandex.net/1/secrets/", strings.NewReader(body))
				require.NoError(t, err)

				return req
			}(),
			ts:    time.Unix(1565681543, 0),
			login: "ppodolsky",
			expectResult: "GET\nhttp://vault-api.passport.yandex.net/1/secrets/?\n{\"tags\":[\"tag1\",\"tag2\"], " +
				"\"query\": \"\xd1\x82\xd0\xb5\xd1\x81\xd1\x82\xd0\xbe\xd0\xb2\xd1\x8b\xd0\xb9 " +
				"\xd1\x81\xd0\xb5\xd0\xba\xd1\x80\xd0\xb5\xd1\x82\"}\n1565681543\nppodolsky\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := serializeRequestV3(tc.request, tc.ts, tc.login)
			require.NoError(t, err)

			assert.Equal(t, tc.expectResult, res)
		})
	}
}
