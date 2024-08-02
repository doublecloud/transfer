package httpyav

import (
	"errors"
	stdlog "log"
	"net/url"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

func TestNewClientWithResty(t *testing.T) {
	defaultURL, _ := url.Parse(DefaultHTTPHost)

	testCases := []struct {
		name         string
		restyClient  *resty.Client
		opts         []ClientOpt
		expectClient *Client
		expectError  error
	}{
		{
			"default_resty_no_opts",
			resty.New(),
			nil,
			func() *Client {
				httpc := resty.New().
					SetBaseURL(DefaultHTTPHost).
					SetHeader("Host", defaultURL.Hostname()).
					SetAllowGetMethodPayload(true).
					SetHeader("User-Agent", "YandexVaultGoClient").
					SetHeader("Content-Type", "application/json")
				return &Client{httpc: httpc}
			}(),
			nil,
		},
		{
			"default_resty_with_opts_error",
			resty.New(),
			[]ClientOpt{
				WithOAuthToken("ololo"),
				func(c *Client) error {
					return errors.New("because I'm bad")
				},
			},
			nil,
			errors.New("because I'm bad"),
		},
		{
			"default_resty_with_opts",
			resty.New(),
			[]ClientOpt{
				WithOAuthToken("trololo"),
				WithHTTPHost("https://vault-api-mock.passport.yandex.net"),
			},
			func() *Client {
				httpc := resty.New().
					SetBaseURL("https://vault-api-mock.passport.yandex.net").
					SetHeader("Host", "vault-api-mock.passport.yandex.net").
					SetHeader("Authorization", "trololo").
					SetAllowGetMethodPayload(true).
					SetHeader("User-Agent", "YandexVaultGoClient").
					SetHeader("Content-Type", "application/json")
				return &Client{httpc: httpc}
			}(),
			nil,
		},
		{
			"custom_resty_with_opts",
			resty.New().
				SetHeader("X-Real-IP", "192.168.0.42"),
			[]ClientOpt{
				WithOAuthToken("trololo"),
				WithHTTPHost("https://vault-api-mock.passport.yandex.net"),
			},
			func() *Client {
				httpc := resty.New().
					SetBaseURL("https://vault-api-mock.passport.yandex.net").
					SetHeader("Host", "vault-api-mock.passport.yandex.net").
					SetHeader("Authorization", "trololo").
					SetHeader("X-Real-IP", "192.168.0.42").
					SetAllowGetMethodPayload(true).
					SetHeader("User-Agent", "YandexVaultGoClient").
					SetHeader("Content-Type", "application/json")
				return &Client{httpc: httpc}
			}(),
			nil,
		},
	}

	// setup comparer
	cmpOpts := cmp.Options{
		cmp.AllowUnexported(Client{}),
		cmpopts.IgnoreUnexported(stdlog.Logger{}, resty.Client{}),
		cmpopts.IgnoreFields(resty.Client{}, "JSONMarshal", "JSONUnmarshal", "XMLMarshal", "XMLUnmarshal"),
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewClientWithResty(tc.restyClient, tc.opts...)

			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.expectError.Error())
			}

			assert.True(t, cmp.Equal(tc.expectClient, c, cmpOpts...), cmp.Diff(tc.expectClient, c, cmpOpts...))
		})
	}
}

func TestNewClient(t *testing.T) {
	defaultURL, _ := url.Parse(DefaultHTTPHost)

	testCases := []struct {
		name         string
		opts         []ClientOpt
		expectClient *Client
		expectError  error
	}{
		{
			"no_opts",
			nil,
			func() *Client {
				httpc := resty.New().
					SetBaseURL(DefaultHTTPHost).
					SetHeader("Host", defaultURL.Hostname()).
					SetAllowGetMethodPayload(true).
					SetHeader("User-Agent", "YandexVaultGoClient").
					SetHeader("Content-Type", "application/json")
				return &Client{httpc: httpc}
			}(),
			nil,
		},
		{
			"with_opts_error",
			[]ClientOpt{
				WithOAuthToken("ololo"),
				func(c *Client) error {
					return errors.New("because I'm bad")
				},
			},
			nil,
			errors.New("because I'm bad"),
		},
		{
			"with_opts",
			[]ClientOpt{
				WithOAuthToken("trololo"),
				WithHTTPHost("https://vault-api-mock.passport.yandex.net"),
			},
			func() *Client {
				httpc := resty.New().
					SetBaseURL("https://vault-api-mock.passport.yandex.net").
					SetHeader("Host", "vault-api-mock.passport.yandex.net").
					SetHeader("Authorization", "trololo").
					SetAllowGetMethodPayload(true).
					SetHeader("User-Agent", "YandexVaultGoClient").
					SetHeader("Content-Type", "application/json")
				return &Client{httpc: httpc}
			}(),
			nil,
		},
	}

	// setup comparer
	cmpOpts := cmp.Options{
		cmp.AllowUnexported(Client{}),
		cmpopts.IgnoreUnexported(stdlog.Logger{}, resty.Client{}),
		cmpopts.IgnoreFields(resty.Client{}, "JSONMarshal", "JSONUnmarshal", "XMLMarshal", "XMLUnmarshal"),
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewClient(tc.opts...)

			if tc.expectError == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.expectError.Error())
			}

			assert.True(t, cmp.Equal(tc.expectClient, c, cmpOpts...), cmp.Diff(tc.expectClient, c, cmpOpts...))
		})
	}
}

func TestClient_Use(t *testing.T) {
	testCases := []struct {
		name        string
		baseClient  *Client
		opts        []ClientOpt
		expectState *Client
		expectPanic bool
	}{
		{
			"with_error",
			func() *Client {
				httpc := resty.New().
					SetBaseURL(DefaultHTTPHost).
					SetHeader("Host", DefaultHTTPHost).
					SetAllowGetMethodPayload(true).
					SetHeader("User-Agent", "YandexVaultGoClient").
					SetHeader("Content-Type", "application/json")
				return &Client{httpc: httpc}
			}(),
			[]ClientOpt{
				func(_ *Client) error {
					return errors.New("omg no")
				},
			},
			func() *Client {
				httpc := resty.New().
					SetBaseURL(DefaultHTTPHost).
					SetHeader("Host", DefaultHTTPHost).
					SetAllowGetMethodPayload(true).
					SetHeader("User-Agent", "YandexVaultGoClient").
					SetHeader("Content-Type", "application/json")
				return &Client{httpc: httpc}
			}(),
			true,
		},
		{
			"refresh_tvm_tickets",
			func() *Client {
				httpc := resty.New().
					SetBaseURL(DefaultHTTPHost).
					SetHeader("Host", DefaultHTTPHost).
					SetAllowGetMethodPayload(true).
					SetHeader("User-Agent", "YandexVaultGoClient").
					SetHeader("Content-Type", "application/json")
				return &Client{httpc: httpc}
			}(),
			[]ClientOpt{
				WithTVMTickets("ololo", "trololo"),
			},
			func() *Client {
				httpc := resty.New().
					SetBaseURL(DefaultHTTPHost).
					SetHeader("Host", DefaultHTTPHost).
					SetAllowGetMethodPayload(true).
					SetHeader("X-Ya-Service-Ticket", "ololo").
					SetHeader("X-Ya-User-Ticket", "trololo").
					SetHeader("User-Agent", "YandexVaultGoClient").
					SetHeader("Content-Type", "application/json")
				return &Client{httpc: httpc}
			}(),
			false,
		},
	}

	// setup comparer
	cmpOpts := cmp.Options{
		cmp.AllowUnexported(Client{}),
		cmpopts.IgnoreUnexported(stdlog.Logger{}, resty.Client{}),
		cmpopts.IgnoreFields(resty.Client{}, "JSONMarshal", "JSONUnmarshal", "XMLMarshal", "XMLUnmarshal"),
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectPanic {
				assert.Panics(t, func() { tc.baseClient.Use(tc.opts...) })
			} else {
				assert.NotPanics(t, func() { tc.baseClient.Use(tc.opts...) })
			}

			assert.True(t, cmp.Equal(tc.expectState, tc.baseClient, cmpOpts...), cmp.Diff(tc.expectState, tc.baseClient, cmpOpts...))
		})
	}
}
