package auth

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/dapr/components-contrib/common/aws/options"
)

const (
	SESSION_DURATION = 3600
)

type AuthOptions struct {
	awsConfig *aws.Config
}

type authOption func(*AuthOptions)

func WithAwsConfig(awsConfig *aws.Config) authOption {
	return func(opts *AuthOptions) {
		opts.awsConfig = awsConfig
	}
}

func loadAuthOptions(opts ...authOption) AuthOptions {
	options := AuthOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

type AuthProvider interface {
	AuthTest() bool
	GetAWSCredentialsProvider() aws.CredentialsProvider
	Close() error
}

func NewAuthProvider(ctx context.Context, opts options.Options) (AuthProvider, error) {
	if authTypeIsx509(opts) {
		return newX509Auth(ctx, opts)
	}
	return newStaticAuth(opts), nil
}
