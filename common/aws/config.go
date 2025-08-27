package aws

import (
	"context"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/dapr/components-contrib/common/aws/auth"
)

type ConfigOption func(*ConfigOptions)

type ConfigOptions struct {
	CredentialProvider aws.CredentialsProvider
	HTTPClient         *http.Client
}

// WithCredentialProvider allows for passing a custom credential provider,
// this is not cached - wrap the credential provider with a NewCredentialCache if you wish.
func WithCredentialProvider(provider aws.CredentialsProvider) func(*ConfigOptions) {
	return func(opts *ConfigOptions) {
		opts.CredentialProvider = provider
	}
}

func WithHTTPClient(client *http.Client) func(*ConfigOptions) {
	return func(opts *ConfigOptions) {
		opts.HTTPClient = client
	}
}

func loadConfigOptions(opts ...ConfigOption) *ConfigOptions {
	options := &ConfigOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

type ConfigLoadOptions []func(*config.LoadOptions) error

func NewConfig(ctx context.Context, authOptions auth.Options, opts ...ConfigOption) (aws.Config, error) {
	options := loadConfigOptions(opts...)

	var configLoadOptions ConfigLoadOptions

	// Deal with options
	if authOptions.Endpoint != "" {
		configLoadOptions = append(
			configLoadOptions,
			config.WithBaseEndpoint(authOptions.Endpoint),
		)
	}

	if authOptions.Region != "" {
		configLoadOptions = append(
			configLoadOptions,
			config.WithRegion(authOptions.Region),
		)
	}

	if options.HTTPClient != nil {
		configLoadOptions = append(
			configLoadOptions,
			config.WithHTTPClient(options.HTTPClient),
		)
	}

	if options.CredentialProvider != nil {
		configLoadOptions = append(
			configLoadOptions,
			config.WithCredentialsProvider(options.CredentialProvider),
		)
	} else {
		credentialsProvider, err := auth.NewCredentialProvider(ctx, authOptions, configLoadOptions)
		if err != nil {
			return aws.Config{}, err
		} else if credentialsProvider.Type() != auth.ProviderTypeUnknown {
			configLoadOptions = append(
				configLoadOptions,
				config.WithCredentialsProvider(aws.NewCredentialsCache(credentialsProvider)),
			)
		}
		// else use the sdk default external config if possible
	}
	return config.LoadDefaultConfig(ctx, configLoadOptions...)
}
