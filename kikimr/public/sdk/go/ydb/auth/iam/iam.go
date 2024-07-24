/*
Package iam provides interface for retrieving and caching iam tokens.
*/package iam

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/ydb/timeutil"
	"github.com/golang-jwt/jwt/v4"
)

// Default client parameters.
const (
	DefaultAudience = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
	DefaultEndpoint = "iam.api.cloud.yandex.net:443"
	DefaultTokenTTL = time.Hour
)

var (
	ErrServiceFileInvalid = errors.New("service account file is not valid")
	ErrKeyCannotBeParsed  = errors.New("private key can not be parsed")
	ErrEndpointRequired   = errors.New("iam: endpoint required")
)

// CreateTokenError contains reason of token creation failure.
type CreateTokenError struct {
	Cause  error
	Reason string
}

// Error implements error interface.
func (e *CreateTokenError) Error() string {
	return fmt.Sprintf("iam: create token error: %s", e.Reason)
}

func (e *CreateTokenError) Unwrap() error {
	return e.Cause
}

type transport interface {
	CreateToken(ctx context.Context, jwt string) (token string, expires time.Time, err error)
}

type ClientOption func(*client) error

// WithEndpoint set provided endpoint.
func WithEndpoint(endpoint string) ClientOption {
	return func(c *client) error {
		c.Endpoint = endpoint
		return nil
	}
}

// WithDefaultEndpoint set endpoint with default value.
func WithDefaultEndpoint() ClientOption {
	return func(c *client) error {
		c.Endpoint = DefaultEndpoint
		return nil
	}
}

// WithSourceInfo set sourceInfo
func WithSourceInfo(sourceInfo string) ClientOption {
	return func(c *client) error {
		c.sourceInfo = sourceInfo
		return nil
	}
}

// WithCertPool set provided certPool.
func WithCertPool(certPool *x509.CertPool) ClientOption {
	return func(c *client) error {
		c.CertPool = certPool
		return nil
	}
}

// WithCertPoolFile try set root certPool from provided cert file path.
func WithCertPoolFile(caFile string) ClientOption {
	return func(c *client) error {
		cp := x509.NewCertPool()
		if err := ydb.AppendCertsFromFile(cp, caFile); err != nil {
			return err
		}
		c.CertPool = cp
		return nil
	}
}

// WithSystemCertPool try set certPool with system root certificates.
func WithSystemCertPool() ClientOption {
	return func(c *client) error {
		var err error
		c.CertPool, err = x509.SystemCertPool()
		return err
	}
}

// WithInsecureSkipVerify set insecureSkipVerify to true which force client accepts any TLS certificate
// presented by the iam server and any host name in that certificate.
//
// If InsecureSkipVerify is set, then certPool field is not used.
//
// This should be used only for testing purposes.
func WithInsecureSkipVerify(insecure bool) ClientOption {
	return func(c *client) error {
		c.InsecureSkipVerify = insecure
		return nil
	}
}

// WithKeyID set provided keyID.
func WithKeyID(keyID string) ClientOption {
	return func(c *client) error {
		c.KeyID = keyID
		return nil
	}
}

// WithIssuer set provided issuer.
func WithIssuer(issuer string) ClientOption {
	return func(c *client) error {
		c.Issuer = issuer
		return nil
	}
}

// WithTokenTTL set provided tokenTTL duration.
func WithTokenTTL(tokenTTL time.Duration) ClientOption {
	return func(c *client) error {
		c.TokenTTL = tokenTTL
		return nil
	}
}

// WithAudience set provided audience.
func WithAudience(audience string) ClientOption {
	return func(c *client) error {
		c.Audience = audience
		return nil
	}
}

// WithPrivateKey set provided private key.
func WithPrivateKey(key *rsa.PrivateKey) ClientOption {
	return func(c *client) error {
		c.Key = key
		return nil
	}
}

// WithPrivateKeyFile try set key from provided private key file path
func WithPrivateKeyFile(path string) ClientOption {
	return func(c *client) error {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		key, err := parsePrivateKey(data)
		if err != nil {
			return err
		}
		c.Key = key
		return nil
	}
}

// WithServiceFile try set key, keyID, issuer from provided service account file path.
//
// Do not mix this option with WithKeyID, WithIssuer and key options (WithPrivateKey, WithPrivateKeyFile, etc).
func WithServiceFile(path string) ClientOption {
	return func(c *client) error {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		type keyFile struct {
			ID               string `json:"id"`
			ServiceAccountID string `json:"service_account_id"`
			PrivateKey       string `json:"private_key"`
		}
		var info keyFile
		if err = json.Unmarshal(data, &info); err != nil {
			return err
		}
		if info.ID == "" || info.ServiceAccountID == "" || info.PrivateKey == "" {
			return ErrServiceFileInvalid
		}

		key, err := parsePrivateKey([]byte(info.PrivateKey))
		if err != nil {
			return err
		}
		c.Key = key
		c.KeyID = info.ID
		c.Issuer = info.ServiceAccountID
		return nil
	}
}

// NewClient creates IAM (jwt) authorized client from provided ClientOptions list.
//
// To create successfully at least one of endpoint options must be provided.
func NewClient(opts ...ClientOption) (ydb.Credentials, error) {
	c := &client{}
	for _, opt := range opts {
		err := opt(c)
		if err != nil {
			return nil, err
		}
	}
	if c.Endpoint == "" {
		return nil, ErrEndpointRequired
	}
	if c.Audience == "" {
		c.Audience = DefaultAudience
	}
	if c.TokenTTL == 0 {
		c.TokenTTL = DefaultTokenTTL
	}
	c.transport = &grpcTransport{
		endpoint:           c.Endpoint,
		certPool:           c.CertPool,
		insecureSkipVerify: c.InsecureSkipVerify,
	}

	return c, nil
}

// Client contains options for interaction with the iam.
type client struct {
	Endpoint string
	CertPool *x509.CertPool

	// If InsecureSkipVerify is true, client accepts any TLS certificate
	// presented by the iam server and any host name in that certificate.
	//
	// If InsecureSkipVerify is set, then CertPool field is not used.
	//
	// This should be used only for testing.
	InsecureSkipVerify bool

	Key    *rsa.PrivateKey
	KeyID  string
	Issuer string

	TokenTTL time.Duration
	Audience string

	once    sync.Once
	mu      sync.RWMutex
	err     error
	token   string
	expires time.Time

	// transport is a stub used for tests.
	transport transport

	sourceInfo string
}

func (c *client) init() (err error) {
	c.once.Do(func() {
		if c.Endpoint == "" {
			c.err = fmt.Errorf("iam: endpoint required")
			return
		}
		if c.Audience == "" {
			c.Audience = DefaultAudience
		}
		if c.TokenTTL == 0 {
			c.TokenTTL = DefaultTokenTTL
		}
		if c.transport == nil {
			c.transport = &grpcTransport{
				endpoint:           c.Endpoint,
				certPool:           c.CertPool,
				insecureSkipVerify: c.InsecureSkipVerify,
			}
		}
	})
	return c.err
}

func (c *client) String() string {
	if c.sourceInfo == "" {
		return "iam.Client"
	}
	return "iam.Client created from " + c.sourceInfo
}

// Token returns cached token if no c.TokenTTL time has passed or no token
// expiration deadline from the last request exceeded. In other way, it makes
// request for a new one token.
func (c *client) Token(ctx context.Context) (token string, err error) {
	if err = c.init(); err != nil {
		return
	}
	c.mu.RLock()
	if !c.expired() {
		token = c.token
	}
	c.mu.RUnlock()
	if token != "" {
		return token, nil
	}
	now := timeutil.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.expired() {
		return c.token, nil
	}
	var expires time.Time
	token, expires, err = c.transport.CreateToken(ctx, c.jwt(now))
	if err != nil {
		return "", &CreateTokenError{
			Cause:  err,
			Reason: err.Error(),
		}
	}
	c.token = token
	c.expires = now.Add(c.TokenTTL)
	if expires.Before(c.expires) {
		c.expires = expires
	}
	return token, nil
}

func (c *client) expired() bool {
	return c.expires.Sub(timeutil.Now()) <= 0
}

// By default Go RSA PSS uses PSSSaltLengthAuto, but RFC states that salt size
// must be equal to hash size.
//
// See https://tools.ietf.org/html/rfc7518#section-3.5
var ps256WithSaltLengthEqualsHash = &jwt.SigningMethodRSAPSS{
	SigningMethodRSA: jwt.SigningMethodPS256.SigningMethodRSA,
	Options: &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
	},
}

func (c *client) jwt(now time.Time) string {
	var (
		issued = now.UTC().Unix()
		expire = now.Add(c.TokenTTL).UTC().Unix()
		method = ps256WithSaltLengthEqualsHash
	)
	t := jwt.Token{
		Header: map[string]interface{}{
			"typ": "JWT",
			"alg": method.Alg(),
			"kid": c.KeyID,
		},
		Claims: jwt.StandardClaims{
			Issuer:    c.Issuer,
			IssuedAt:  issued,
			Audience:  c.Audience,
			ExpiresAt: expire,
		},
		Method: method,
	}
	s, err := t.SignedString(c.Key)
	if err != nil {
		panic(fmt.Sprintf("iam: could not sign jwt token: %v", err))
	}
	return s
}

func parsePrivateKey(raw []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, ErrKeyCannotBeParsed
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return key, err
	}

	x, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	if key, ok := x.(*rsa.PrivateKey); ok {
		return key, nil
	}
	return nil, ErrKeyCannotBeParsed
}
