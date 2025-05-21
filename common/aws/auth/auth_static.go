package auth

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/dapr/components-contrib/common/aws/options"
	"github.com/dapr/kit/logger"
)

type AuthStatic struct {
	logger logger.Logger

	region       *string
	endpoint     *string // aka BaseEndpoint
	accessKey    *string
	secretKey    *string
	sessionToken *string

	assumeRoleArn *string
	sessionName   *string
}

func newStaticAuth(opts options.Options) AuthProvider {
	provider := &AuthStatic{
		logger: opts.Logger,
	}

	switch {
	case opts.AccessKey != "":
		provider.accessKey = &opts.AccessKey
	case opts.AssumeRoleARN != "":
		provider.assumeRoleArn = &opts.AssumeRoleARN
	case opts.Endpoint != "":
		provider.endpoint = &opts.Endpoint
	case opts.Region != "":
		provider.region = &opts.Region
	case opts.SecretKey != "":
		provider.secretKey = &opts.SecretKey
	case opts.SessionName != "":
		provider.sessionName = &opts.SessionName
	case opts.SessionToken != "":
		provider.sessionToken = &opts.SessionToken
	}

	return provider
}

func (a *AuthStatic) AuthTest() bool {
	//TODO implement me
	panic("implement me")
}

func (a *AuthStatic) GetAWSCredentialsProvider() aws.CredentialsProvider {
	switch {
	case a.accessKey == nil:
		a.logger.Error("accessKey is nil")
		return nil
	case a.secretKey == nil:
		a.logger.Error("secretKey is nil")
		return nil
	case a.sessionToken == nil:
		a.logger.Error("sessionToken is nil")
		return nil
	}

	cpo := credentials.NewStaticCredentialsProvider(*a.accessKey, *a.secretKey, *a.sessionToken)

	return aws.NewCredentialsCache(cpo) // cache the credentials
}

func (a *AuthStatic) Close() error {
	//TODO implement me
	panic("implement me")
}
