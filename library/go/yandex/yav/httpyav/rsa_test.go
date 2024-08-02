package httpyav

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPrivateRSAKey = `
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA63Qr2tsZPe9kQJdQE9im/dQ2TU/fc5mt+TD7IdHI6F3NzIlh
m8wX4ferxzma5WUk2sTJpzlNcnHBh7cBAyJMJo0knktKvfZD7uHcxPBrhlHUV58D
HPwpXM9znJaElNQoZXPQ3y/dmvY9ym0khGNCDjGjI5z9C8ATw8+D7Y6DS3ZC5oOQ
Q5r4f/K2sKbVakUutqunkMR14Non5O1abtqjoYCyCAXP08rdj3ooY6I/maOty/yU
I2Xg6NHpP5Q/YvskZPNxB1M3VMh0WZPIX0urb1Qzam/zScM6Ew31nsh55gPQ8r8j
6wkPBk/j0xhdtl53tLTDj317C8BHEGTPmdPhdQIDAQABAoIBAFyMLTC5LhLKJf29
fBxQ7FKZNz7sRkiJ/3gTaKLCctXjCSF8XoF+l2SalUqZueiw+OuErj6sp2R0kj1m
EV/J+2Sr1djif15rjgg3fy9p0NnbEDvgpLif5SI16JuEDljxi29VNqSDi/d9Eoye
mdvvp+csW5OEAXK87QfqaVDW04S1FlKpQqXv3If0MSmt8J26IUUMWeTXfIKGBIGx
N4gfbFlrLxoediyDUFv0CE5bSQpTeEe6q+e1iMdrWXZHA8gWtdU2MEw+v7wcKju7
oP//Jp/Eg8pXmoWC80lje+Tb3pL9WHRMR2yt89Q81nXvIBRVHWSqvWZLfDEWjUoT
IGNYgAECgYEA+CSdesEmh4xKMrQTo95zTomm6vXZ+gJe9gvS24F6lx3DN9x9LMrl
cK1WOSGVus2feVDxjVz4F8Mdu8hPE9BZWhKzSg2AqBXG5t1htKaSQ2D8Jrks3byc
hjIpo5Zzqen/FUpFiA1IxlrSMK8z0o0wXsGRXVoc6+0fskMRluE3owECgYEA8uiz
Tus3ClCrcjL2ycIX2TAQMhlP6pjPt1YvO+SPjBvVj74ArwKZxPrm1XmqVqEekvpg
sdeWU9S6h3J+yhxi2DIDCgM2nUotYBcCaFs6Ir11RXgQyjYKeJsFV1sYrImTFsCY
0QtVG87xWZeYAHkby2qhT8dBr+iyZ/TBJl8AYnUCgYBWhF2r6SBH7nAIUaTvY6YM
Yg4iqemAM8dsPh8cjX5ypdvk5Cl4rp1kter0LHOKGBtcLw6pXRrbHhqF2IdJv0EI
GLEORrru3/jjkZh5ZgJlH7GKxtGP1i001NSTxuc4/O8FO0oW75rKHexfMRb+eF+/
Cfpm8/5Ve+2rN5swYgIGAQKBgCT9grC16P/NIQ6W7DX1NKSCSTUX3a+f7aHBohfA
yotPgcoN6RS9lKUGgDhp+qKOjpVbQ3ZRmjbR4kXWDbDBedvqYcQYkSyKqzZCyr8R
hVzc9QrLKeNhL18GXF3dJXjAyoFgeuT6kM9XSDGYgDEyQCVN65q2gS5EhUaHYxJw
zSIxAoGBAIUHxgTXBStKvvO31Cu2MHLl3VwnNu+7FTQHSArMTndv6BPOJZha8+Bc
U2eCXNWVYzd7dGObEac3iX/tGi7BFKgj3DUgCHpibE2GsByb9GCwRK7ELGMb54HU
S6YpUSO7mgyX1eqSBfcFx0+KOtvk1CvqnxcF+ImyvqkLuDTI120h
-----END RSA PRIVATE KEY-----
`

func TestComputeRSASignature(t *testing.T) {
	testCases := []struct {
		serialized      string
		privKey         *rsa.PrivateKey
		expectSignature string
	}{
		{
			serialized: "GET\nhttp://vault-api.passport.yandex.net/1/secrets/\n{\"tags\": \"tag1,tag2\", " +
				"\"query\": \"\xd1\x82\xd0\xb5\xd1\x81\xd1\x82\xd0\xbe\xd0\xb2\xd1\x8b\xd0\xb9 " +
				"\xd1\x81\xd0\xb5\xd0\xba\xd1\x80\xd0\xb5\xd1\x82\"}\n1565681543\nppodolsky\n",
			privKey: func() *rsa.PrivateKey {
				privPem, _ := pem.Decode([]byte(testPrivateRSAKey))
				pk, err := x509.ParsePKCS1PrivateKey(privPem.Bytes)
				require.NoError(t, err)
				return pk
			}(),
			expectSignature: "AAAAB3NzaC1yc2EAAAEAABtXq5x7974JjvHNcXktjIMVL6EK8y9QPXh_n14l8gHvhCI87di_n9-xIQ2MGz-hhgwOR" +
				"bGcUY-bXF68OJYeO9c2J2VP-ra0EBs1YMBEqxwUp0h9IT4irQY_p8CTheZu9I4cLEFW7IQ1PBPrW-L7LZ8myvR7ED" +
				"-WfRLLn87G50_VFOdrHbXYz0J1EbERNqqv1p3V0BkUva1_P9fz_E6mqd-NkBpiHRALXGMp3mXyJSMHGGiwK4H8zrJ" +
				"rEg4qIhyWJ_AgX_rs2FlbLeFJhR4NnmTFzgTrxwt0RRJZ07ZMAjqGkb7GvCJTQdE2vZYyHmQCxr0iIgEVEIKumxMu" +
				"dqLdHA==",
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			sig, err := computeRSASignature(tc.serialized, tc.privKey)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectSignature, sig)
		})
	}
}

// see: https://github.com/doublecloud/tross/arc/trunk/arcadia/passport/infra/daemons/vault/api/utils/secrets.py?rev=r9154062#L29
func TestDefaultHash(t *testing.T) {
	testCases := []struct {
		input  string
		expect []byte
	}{
		{
			input:  "test",
			expect: []byte(`9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08`),
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			res := defaultHash(tc.input)
			assert.Equal(t, tc.expect, res)
		})
	}
}
