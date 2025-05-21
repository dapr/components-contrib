package aws

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/dapr/components-contrib/common/aws/options"
)

func AWSConfigFromOptions(ctx context.Context, opts options.Options, awsOpts ...func(*config.LoadOptions) error) *aws.Config {
	options := []func(*config.LoadOptions) error{}
	switch {
	case opts.Region != "":
		options = append(options, config.WithRegion(opts.Region))
	case opts.Endpoint != "":
		options = append(options, config.WithBaseEndpoint(opts.Endpoint))
	}

	options = append(options, awsOpts...)

	awsConfig, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		// TODO: @mikeee - refactor this function to return an error or use the logger
		return nil
	}

	return &awsConfig
}
