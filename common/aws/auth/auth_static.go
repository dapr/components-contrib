package auth

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/dapr/kit/logger"
)

type Static struct {
	ProviderType          ProviderType
	Logger                logger.Logger
	AccessKey             string
	SecretKey             string
	SessionToken          string
	Region                string
	Endpoint              string
	AssumeRoleArn         string
	AssumeRoleSessionName string

	CredentialProvider aws.CredentialsProvider
}

func (a *Static) Retrieve(ctx context.Context) (aws.Credentials, error) {
	if a.CredentialProvider == nil {
		return aws.Credentials{}, errors.New("credential provider is not set")
	}
	return a.CredentialProvider.Retrieve(ctx)
}

func (a *Static) Type() ProviderType {
	return a.ProviderType
}

func newAuthStatic(ctx context.Context, opts Options, configOpts []func(*config.LoadOptions) error) (CredentialProvider, error) {
	static := &Static{
		Logger:                opts.Logger,
		AccessKey:             opts.AccessKey,
		SecretKey:             opts.SecretKey,
		SessionToken:          opts.SessionToken,
		Region:                opts.Region,
		Endpoint:              opts.Endpoint,
		AssumeRoleArn:         opts.AssumeRoleArn,
		AssumeRoleSessionName: opts.AssumeRoleSessionName,
	}

	switch {
	case static.AccessKey != "" && static.SecretKey != "":
		static.ProviderType = StaticProviderTypeStatic
		static.CredentialProvider = credentials.NewStaticCredentialsProvider(opts.AccessKey, opts.SecretKey,
			opts.SessionToken)
		static.Logger.Debug("using static credentials provider")

	case static.AssumeRoleArn != "":
		awsCfg, err := config.LoadDefaultConfig(ctx, configOpts...)
		if err != nil {
			return nil, err
		}

		stsSvc := sts.NewFromConfig(awsCfg)
		stsProvider := stscreds.NewAssumeRoleProvider(stsSvc, static.AssumeRoleArn, func(options *stscreds.AssumeRoleOptions) {
			options.RoleSessionName = static.AssumeRoleSessionName
		})
		static.ProviderType = StaticProviderTypeAssumeRole
		static.CredentialProvider = stsProvider
		static.Logger.Debug("using AssumeRole credentials provider")

	default:
		static.Logger.Debug("using an undefined credentials provider, this may lead to unexpected behavior")
		static.ProviderType = ProviderTypeUnknown
	}

	return static, nil
}
