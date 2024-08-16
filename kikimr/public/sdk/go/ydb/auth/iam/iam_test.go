package iam

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/doublecloud/transfer/kikimr/public/sdk/go/ydb/timeutil"
	"github.com/golang-jwt/jwt/v4"
)

type TransportFunc func(context.Context, string) (string, time.Time, error)

func (f TransportFunc) CreateToken(ctx context.Context, jwt string) (string, time.Time, error) {
	return f(ctx, jwt)
}

func TestClientToken(t *testing.T) {
	const (
		keyID    = "key-id"
		issuer   = "issuer"
		audience = "audience"
		endpoint = "endpoint"

		ttl = time.Minute
	)
	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		t.Error(err)
	}

	prevTimeFunc := jwt.TimeFunc
	jwt.TimeFunc = timeutil.Now
	shiftTime, cleanup := timeutil.StubTestHookTimeNow(time.Unix(10, 0))
	defer func() {
		cleanup()
		jwt.TimeFunc = prevTimeFunc
	}()

	var (
		i int

		results = [...]struct {
			token   string
			expires time.Duration
		}{
			{"foo", ttl},
			{"bar", time.Second},
			{"baz", 0},
		}
	)
	c := client{
		Endpoint: endpoint,
		Key:      key,
		KeyID:    keyID,
		Issuer:   issuer,

		Audience: audience,
		TokenTTL: ttl,

		// Stub the real transport logic to check jwt token for correctness.
		transport: TransportFunc(func(ctx context.Context, jwts string) (
			string, time.Time, error,
		) {
			var claims jwt.StandardClaims
			keyFunc := func(t *jwt.Token) (interface{}, error) {
				// Use the public part of our key as IAM service will.
				return key.Public(), nil
			}
			token, err := jwt.ParseWithClaims(jwts, &claims, keyFunc)
			if err != nil {
				t.Errorf("parse token error: %v", err)
			}
			if act, exp := token.Header["kid"], keyID; act != exp {
				t.Errorf("unexpected \"kid\" header: %+q; want %+q", act, exp)
			}

			// Get the "now" moment. Note that this is the same as for sourceInfo –
			// we stubbed time above.
			now := timeutil.Now()

			iat := now.UTC().Unix()
			exp := now.UTC().Add(ttl).Unix()

			if act, exp := claims.Issuer, issuer; act != exp {
				t.Errorf("unexpected claims.Issuer field: %+q; want %+q", act, exp)
			}
			if act, exp := claims.Audience, audience; act != exp {
				t.Errorf("unexpected claims.Audience field: %+q; want %+q", act, exp)
			}
			if act, exp := claims.IssuedAt, iat; act != exp {
				t.Errorf("unexpected claims.IssuedAt field: %+q; want %+q", act, exp)
			}
			if act, exp := claims.ExpiresAt, exp; act != exp {
				t.Errorf("unexpected claims.ExpiresAt field: %+q; want %+q", act, exp)
			}

			t := results[i].token
			e := results[i].expires
			i++

			return t, now.Add(e), nil
		}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempt int
	getToken := func(expResult int) {
		t1, err := c.Token(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if act, exp := t1, results[expResult].token; act != exp {
			t.Errorf(
				"#%d Token(): unexpected token: %v; want %v",
				attempt, act, exp,
			)
		}
		attempt++
	}

	getToken(0)

	shiftTime(time.Second)
	getToken(0)

	shiftTime(ttl) // time.Minute
	getToken(1)

	// Now server respond with time.Second expiration time.
	// Thus we expect Token() request server again after second, not after
	// ttl (which is time.Minute).
	shiftTime(time.Second)
	getToken(2)
}

func TestOptionsConfig(t *testing.T) {
	const (
		keyID    = "key-id"
		issuer   = "issuer"
		audience = "audience"
		endpoint = "endpoint"

		ttl = time.Minute
	)
	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		t.Error(err)
	}

	c, err := NewClient(
		WithKeyID(keyID),
		WithIssuer(issuer),
		WithAudience(audience),
		WithEndpoint(endpoint),
		WithTokenTTL(ttl),
		WithPrivateKey(key),
	)
	if err != nil {
		t.Error(err)
	}
	if cl, ok := c.(*client); !ok ||
		cl.KeyID != keyID ||
		cl.Issuer != issuer ||
		cl.Audience != audience ||
		cl.Endpoint != endpoint ||
		cl.TokenTTL != ttl ||
		cl.Key != key {
		t.Error("client object doesn't match")
	}

}
