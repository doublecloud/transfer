package config

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/doublecloud/tross/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	yckms "github.com/doublecloud/tross/cloud/bitbucket/private-api/yandex/cloud/priv/kms/v1"
	"github.com/doublecloud/tross/cloud/bitbucket/public-api/yandex/cloud/lockbox/v1"
	"github.com/doublecloud/tross/library/go/core/xerrors"
	"github.com/doublecloud/tross/library/go/yandex/yav/httpyav"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/token"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/util/rolechain"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/yc"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type YavSecretResolver interface {
	ResolveSecret(secret yavSecret) (string, error)
}

type AWSSecretResolver interface {
	ResolveSecret(ciphertext string) (string, error)
}

type KMSSecretResolver interface {
	ResolveSecret(ciphertext string) (string, error)
}

type MetadataSecretResolver interface {
	ResolveSecret(userDataField string) (string, error)
}

type LockboxSecretResolver interface {
	ResolveSecret(secretID, key string) (string, error)
}

type AWSSecretManagerResolver interface {
	ResolveSecret(secretID, keyID string) (string, error)
}

type DefaultYavSecretResolver struct{ *httpyav.Client }

func (r *DefaultYavSecretResolver) ResolveSecret(secret yavSecret) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	uuid := secret.SecretID
	if secret.Version != "" {
		uuid = secret.Version
	}
	response, err := r.GetVersion(ctx, uuid)
	if err != nil {
		return "", err
	}
	if response.Err() != nil {
		return "", xerrors.Errorf("Yav error: %w", response.Err())
	}
	for _, secretValue := range response.Version.Values {
		if secretValue.Key == secret.Key {
			switch secretValue.Encoding {
			case "":
				return secretValue.Value, nil
			case "base64":
				decodedSecret, err := base64.StdEncoding.DecodeString(secretValue.Value)
				if err != nil {
					return "", xerrors.Errorf("Cannot decode base64 value at key %s: %w", secretValue.Key, err)
				}
				return string(decodedSecret), nil
			}
			return "", xerrors.Errorf("Unknown secret encoding at key %s: %s", secretValue.Key, secretValue.Encoding)
		}
	}
	return "", xerrors.Errorf("Key %s not found", secret.Key)
}

type DefaultAWSResolver func(ciphertext string) (string, error)

func (r DefaultAWSResolver) ResolveSecret(ciphertext string) (string, error) {
	return r(ciphertext)
}

type DefaultKMSResolver func(ciphertext string) (string, error)

func (r DefaultKMSResolver) ResolveSecret(ciphertext string) (string, error) {
	return r(ciphertext)
}

type DefaultLockboxResolver func(secretID, key string) (string, error)

func (r DefaultLockboxResolver) ResolveSecret(secretID, key string) (string, error) {
	return r(secretID, key)
}

type DefaultAWSSecretManagerResolver func(secretID, key string) (string, error)

func (r DefaultAWSSecretManagerResolver) ResolveSecret(secretID, keyID string) (string, error) {
	return r(secretID, keyID)
}

type DefaultMetadataResolver func(userdataField string) (string, error)

func (r DefaultMetadataResolver) ResolveSecret(userDataField string) (string, error) {
	return r(userDataField)
}

type SecretResolverFactory interface {
	NewYavResolver(configBundle ConfigBundle) (YavSecretResolver, error)
	NewKMSResolver(configBundle ConfigBundle) (KMSSecretResolver, error)
	NewMetadataResolver(configBundle ConfigBundle) (MetadataSecretResolver, error)
	NewAWSResolver(configBundle ConfigBundle) (AWSSecretResolver, error)
	NewAWSSecretManagerResolver(configBundle ConfigBundle) (AWSSecretManagerResolver, error)
	NewLockboxResolver(configBundle ConfigBundle) (LockboxSecretResolver, error)
}

func NewYavResolver(yavToken string) (YavSecretResolver, error) {
	yavClient, err := httpyav.NewClient(httpyav.WithOAuthToken(yavToken))
	if err != nil {
		return nil, err
	}
	return &DefaultYavSecretResolver{Client: yavClient}, nil
}

func NewKMSResolver(cloudCreds CloudCreds, yandexCloudEndpoint, kmsKeyID, kmsEndpoint string) (KMSSecretResolver, error) {
	sdk, err := buildYCSDK(yandexCloudEndpoint, cloudCreds)
	if err != nil {
		return nil, xerrors.Errorf("Cannot build YC SDK: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	defer func() { _ = sdk.Shutdown(ctx) }()
	iamResponse, err := sdk.CreateIAMToken(ctx)
	if err != nil {
		return nil, xerrors.Errorf("Cannot create IAM token: %w", err)
	}
	kmsConn, err := grpc.Dial(kmsEndpoint, grpc.WithTransportCredentials(credentials.NewTLS(new(tls.Config))))
	if err != nil {
		return nil, xerrors.Errorf("Cannot dial %s: %w", kmsEndpoint, err)
	}
	kmsClient := yckms.NewSymmetricCryptoServiceClient(kmsConn)
	return DefaultKMSResolver(func(ciphertext string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		requestCtx := metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{"authorization": fmt.Sprintf("Bearer %s", iamResponse.IamToken)}))
		request := &yckms.SymmetricDecryptRequest{
			KeyId:      kmsKeyID,
			Ciphertext: []byte(ciphertext),
		}
		response, err := kmsClient.Decrypt(requestCtx, request)
		if err != nil {
			return "", xerrors.Errorf("unable to decrypt key: %v: %w", kmsKeyID, err)
		}
		return string(response.Plaintext), nil
	}), nil
}

func NewLockboxResolver(cloudCreds CloudCreds, yandexCloudEndpoint string) (LockboxSecretResolver, error) {
	return DefaultLockboxResolver(func(secretID, key string) (string, error) {
		sdk, err := buildYCSDK(yandexCloudEndpoint, cloudCreds)
		if err != nil {
			return "", xerrors.Errorf("Cannot build YC SDK: %w", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		defer func() { _ = sdk.Shutdown(ctx) }()

		client, err := sdk.LockboxPayloadServiceClient(ctx)
		if err != nil {
			return "", xerrors.Errorf("unable to crete lockbox service: %w", err)
		}
		request := &lockbox.GetPayloadRequest{SecretId: secretID}
		iamResponse, err := sdk.CreateIAMToken(ctx)
		if err != nil {
			return "", xerrors.Errorf("unable to get IAM token: %w", err)
		}

		ctx = yc.WithUserAuth(token.WithToken(ctx, iamResponse.IamToken))
		payload, err := client.Get(yc.WithUserAuth(token.WithToken(ctx, iamResponse.IamToken)), request)
		if err != nil {
			return "", xerrors.Errorf("unable to get lockbox secret payload: %w", err)
		}

		for _, entry := range payload.Entries {
			if entry.Key != key {
				continue
			}
			switch value := entry.Value.(type) {
			case *lockbox.Payload_Entry_TextValue:
				return value.TextValue, nil
			case *lockbox.Payload_Entry_BinaryValue:
				return string(value.BinaryValue), nil
			default:
				return "", xerrors.Errorf("unsupported value type: %T", value)
			}
		}
		return "", xerrors.Errorf("no key %s found in secret %s", key, secretID)
	}), nil
}

func NewAWSResolver(awsKeyID, profile, roleToAssume string, assumeChain ...string) (AWSSecretResolver, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: aws.String("eu-central-1"), Credentials: NewAWSCreds(roleToAssume)},
		Profile:           profile,
	}))
	if r, err := sts.New(sess).GetCallerIdentity(&sts.GetCallerIdentityInput{}); err != nil {
		logger.Log.Warnf("unable to resolve current caller identity: error: %v roleToAssume: %s assumeChain: %v", err, roleToAssume, assumeChain)
	} else {
		logger.Log.Infof("current ARN: %s", *r.Arn)
	}

	// Create KMS service client
	if len(assumeChain) > 0 {
		sess = rolechain.NewSession(sess, assumeChain...)
	}
	if r, err := sts.New(sess).GetCallerIdentity(&sts.GetCallerIdentityInput{}); err != nil {
		logger.Log.Warnf("unable to resolve current caller identity: %v", err)
	} else {
		logger.Log.Infof("ARN after assume chain: %s", *r.Arn)
	}
	svc := kms.New(sess, aws.NewConfig().WithRegion("eu-central-1"))

	return DefaultAWSResolver(func(ciphertext string) (string, error) {
		result, err := svc.Decrypt(&kms.DecryptInput{CiphertextBlob: []byte(ciphertext), KeyId: &awsKeyID})
		if err != nil {
			identity := ""
			if r, err := sts.New(sess).GetCallerIdentity(&sts.GetCallerIdentityInput{}); err != nil {
				logger.Log.Warnf("unable to resolve current caller identity: %v", err)
			} else {
				identity = *r.Arn
			}
			return "", xerrors.Errorf("unable to decrypt key: %v to %s (with %s profile): %w", awsKeyID, identity, profile, err)
		}
		return string(result.Plaintext), nil
	}), nil
}

func NewAWSSecretManagerResolver(profile, roleToAssume string, assumeChain ...string) (AWSSecretManagerResolver, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config:            aws.Config{Region: aws.String("eu-central-1"), Credentials: NewAWSCreds(roleToAssume)},
		Profile:           profile,
	}))

	if len(assumeChain) > 0 {
		sess = rolechain.NewSession(sess, assumeChain...)
	}
	sm := secretsmanager.New(sess, aws.NewConfig().WithRegion("eu-central-1"))

	return DefaultAWSSecretManagerResolver(func(secretID, key string) (string, error) {
		val, err := sm.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretId: aws.String(secretID)})
		if err != nil {
			return "", xerrors.Errorf("unable to extract secret ID: %s: %w", secretID, err)
		}
		kv := map[string]any{}
		if err := json.Unmarshal([]byte(*val.SecretString), &kv); err != nil {
			return "", xerrors.Errorf("unable to extract secret key-value: %w", err)
		}
		v, ok := kv[key]
		if !ok {
			return "", xerrors.Errorf("unable to find key: %s: in secret: %s, known keys: %v", key, secretID, maps.Keys(kv))
		}
		return fmt.Sprintf("%v", v), nil
	}), nil
}

func NewMetadataResolver() (MetadataSecretResolver, error) {
	return DefaultMetadataResolver(func(userDataField string) (string, error) {
		resultTypeErased, err := getUserDataField(userDataField)
		if err != nil {
			return "", xerrors.Errorf("Cannot get user data field %s: %w", userDataField, err)
		}
		result, ok := resultTypeErased.(string)
		if !ok {
			return "", xerrors.Errorf("Expected string, not %T", resultTypeErased)
		}
		return result, nil
	}), nil
}

func buildYCSDK(yandexCloudEndpoint string, credData CloudCreds) (sdk *yc.SDK, err error) {
	var credentials yc.Credentials
	switch credData := credData.(type) {
	case *PassportOauthToken:
		if credData.Token == "" {
			return nil, xerrors.Errorf("No cloud OAuth token given")
		}
		credentials = yc.OAuthToken(string(credData.Token))
	case *ServiceAccountKey:
		if credData.PrivateKey == "" {
			return nil, xerrors.Errorf("No private key given")
		}
		keyAlgo, ok := iam.Key_Algorithm_value[credData.KeyAlgorithm]
		if !ok || keyAlgo == int32(iam.Key_ALGORITHM_UNSPECIFIED) {
			return nil, xerrors.Errorf("Invalid service account key algorithm: %s", credData.KeyAlgorithm)
		}
		credentials, err = yc.ServiceAccountKey(&iam.Key{
			Id: credData.ID,
			Subject: &iam.Key_ServiceAccountId{
				ServiceAccountId: credData.ServiceAccountID,
			},
			KeyAlgorithm: iam.Key_Algorithm(keyAlgo),
			PublicKey:    credData.PublicKey,
		}, string(credData.PrivateKey))
		if err != nil {
			return nil, xerrors.Errorf("Cannot create cloud credentials from service account key: %w", err)
		}
	case *InstanceMetadata:
		credentials = yc.InstanceServiceAccount()
	case *ConstantIAMToken:
		credentials = credData
	case *YcProfile:
		credentials = credData
	}
	if credentials == nil {
		return nil, xerrors.Errorf("Unknown credentials type: %T", credData)
	}
	return yc.Build(yc.MakeConfig(credentials, yandexCloudEndpoint))
}

var (
	_ SecretResolverFactory = (*MockSecretResolverFactory)(nil)
)

type MockSecretResolverFactory struct{ *testing.T }

type MockYavSecretResolver struct{ *testing.T }
type MockKMSSecretResolver struct{ *testing.T }
type MockMetadataSecretResolver struct{ *testing.T }
type MockAWSSecretResolver struct{ *testing.T }
type MockLockboxSecretResolver struct{ *testing.T }
type MockSecretManagerResolver struct{ *testing.T }

func (f MockSecretResolverFactory) NewAWSSecretManagerResolver(configBundle ConfigBundle) (AWSSecretManagerResolver, error) {
	return MockSecretManagerResolver(f), nil
}

func (f MockSecretResolverFactory) NewYavResolver(configBundle ConfigBundle) (YavSecretResolver, error) {
	return MockYavSecretResolver(f), nil
}

func (f MockSecretResolverFactory) NewKMSResolver(configBundle ConfigBundle) (KMSSecretResolver, error) {
	return MockKMSSecretResolver(f), nil
}

func (f MockSecretResolverFactory) NewMetadataResolver(configBundle ConfigBundle) (MetadataSecretResolver, error) {
	return MockMetadataSecretResolver(f), nil
}

func (f MockSecretResolverFactory) NewAWSResolver(configBundle ConfigBundle) (AWSSecretResolver, error) {
	return MockAWSSecretResolver(f), nil
}

func (f MockSecretResolverFactory) NewLockboxResolver(configBundle ConfigBundle) (LockboxSecretResolver, error) {
	return MockLockboxSecretResolver(f), nil
}

func (r MockYavSecretResolver) ResolveSecret(secret yavSecret) (string, error) {
	const secretIDpattern = "sec-[0-9a-z]{26}"
	const versionIDpattern = "ver-[0-9a-z]{26}"
	matched, err := regexp.MatchString(secretIDpattern, secret.SecretID)
	if err != nil {
		panic(err)
	}
	if !matched {
		require.Failf(r, "Secret ID %s does not match pattern %s", secret.SecretID, secretIDpattern)
	}
	if secret.Version != "" {
		matched, err = regexp.MatchString(versionIDpattern, secret.Version)
		if err != nil {
			panic(err)
		}
		if !matched {
			require.Failf(r, "Secret version %s does not match pattern %s", secret.Version, secretIDpattern)
		}
		return "resolved:" + secret.Version, nil
	}
	return "resolved:" + secret.SecretID, nil
}

func (r MockKMSSecretResolver) ResolveSecret(ciphertext string) (string, error) {
	return "resolved:" + ciphertext, nil
}

func (r MockMetadataSecretResolver) ResolveSecret(userDataField string) (string, error) {
	return "resolved:" + userDataField, nil
}

func (r MockAWSSecretResolver) ResolveSecret(userDataField string) (string, error) {
	return "resolved:" + userDataField, nil
}

func (m MockLockboxSecretResolver) ResolveSecret(secretID, key string) (string, error) {
	return "resolved:" + secretID, nil
}

func (m MockSecretManagerResolver) ResolveSecret(secretID, keyID string) (string, error) {
	return "resolved:" + secretID + keyID, nil
}
