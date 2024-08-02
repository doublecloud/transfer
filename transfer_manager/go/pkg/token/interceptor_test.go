package token

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestInterceptorWithOAuth(t *testing.T) {
	oauthTokenExpected := "fake oauth"
	tokenExpected := "fake token"

	t.Run("no prefix", func(t *testing.T) {
		testInterceptorWithOAuth(t, oauthTokenExpected, oauthTokenExpected, tokenExpected)
	})

	oauthTokenPrefixes := []string{"oauth ", "OAuth "}
	for _, oauthTokenPrefix := range oauthTokenPrefixes {
		t.Run(fmt.Sprintf("prefix '%v'", oauthTokenPrefix), func(t *testing.T) {
			authorization := oauthTokenPrefix + oauthTokenExpected
			testInterceptorWithOAuth(t, authorization, oauthTokenExpected, tokenExpected)
		})
	}
}

func testInterceptorWithOAuth(t *testing.T, authorization string, oauthTokenExpected string, tokenExpected string) {
	md := metadata.New(map[string]string{
		"authorization": authorization,
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	provider := NewProviderMock(func(ctx context.Context, oauthToken string) (string, error) {
		require.Equal(t, oauthTokenExpected, oauthToken)
		return tokenExpected, nil
	})
	interceptor := NewInterceptor(NewInterceptorConfig(func(info *grpc.UnaryServerInfo) bool {
		return false
	}), provider)
	_, err := interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		token, ok := FromContext(ctx)
		require.True(t, ok)
		require.Equal(t, tokenExpected, token)
		return nil, nil
	})
	require.NoError(t, err)
}

func TestInterceptorWithIAM(t *testing.T) {
	tokenExpected := "fake token"

	iamTokenPrefixes := []string{"iam ", "IAM ", "bearer ", "Bearer "}
	for _, iamTokenPrefix := range iamTokenPrefixes {
		t.Run(fmt.Sprintf("prefix '%v'", iamTokenPrefix), func(t *testing.T) {
			authorization := iamTokenPrefix + tokenExpected
			testInterceptorWithIAM(t, authorization, tokenExpected)
		})
	}
}

func testInterceptorWithIAM(t *testing.T, authorization string, tokenExpected string) {
	md := metadata.New(map[string]string{
		"authorization": authorization,
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	provider := NewProviderMock(func(ctx context.Context, oauthToken string) (string, error) {
		require.Fail(t, "invalid provider call")
		return "", nil
	})
	interceptor := NewInterceptor(NewInterceptorConfig(func(info *grpc.UnaryServerInfo) bool {
		return false
	}), provider)
	_, err := interceptor.Intercept(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
		token, ok := FromContext(ctx)
		require.True(t, ok)
		require.Equal(t, tokenExpected, token)
		return nil, nil
	})
	require.NoError(t, err)
}
