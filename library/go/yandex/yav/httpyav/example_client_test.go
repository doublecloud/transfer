package httpyav_test

import (
	"context"

	"github.com/doublecloud/tross/library/go/yandex/yav"
	"github.com/doublecloud/tross/library/go/yandex/yav/httpyav"
)

func ExampleNewClient() {
	// using OAuth token
	{
		oauthToken := "<my_oauth_token>"

		yc, err := httpyav.NewClient(
			httpyav.WithOAuthToken(oauthToken),
		)

		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		resp, err := yc.CreateSecret(ctx, yav.SecretRequest{
			Name:    "my_super_secret",
			Comment: "pssss, dont tell anyone",
			Tags:    []string{"my_secret", "very_secret_secret"},
		})

		if err != nil {
			panic(err)
		}

		if resp.Status == yav.StatusError {
			panic(resp.Err())
		}

		_ = resp.SecretUUID
	}

	// using TVM tickets
	{
		serviceTicket := "<my_service_fresh_tvm_ticket>"
		userTicket := "<my_user_fresh_tvm_ticket>"

		yc, err := httpyav.NewClient()

		if err != nil {
			panic(err)
		}

		ctx := context.Background()

		// TVM tickets have short lifetime, so we need to pass fresh tickets on every request
		resp, err := yc.Use(httpyav.WithTVMTickets(
			serviceTicket, userTicket,
		)).CreateSecret(ctx, yav.SecretRequest{
			Name:    "my_super_secret",
			Comment: "pssss, dont tell anyone",
			Tags:    []string{"my_secret", "very_secret_secret"},
		})

		if err != nil {
			panic(err)
		}

		if resp.Status == yav.StatusError {
			panic(resp.Err())
		}

		_ = resp.SecretUUID
	}
}
