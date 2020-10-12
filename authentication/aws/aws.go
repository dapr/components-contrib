package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

func GetClient(accessKey string, secretKey string, region string, endpoint string) (*session.Session, error) {
	awsConfig := aws.NewConfig()

	if region != "" {
		awsConfig = awsConfig.WithRegion(region)
	}
	if accessKey != "" && secretKey != "" {
		awsConfig = awsConfig.WithCredentials(credentials.NewStaticCredentials(accessKey, secretKey, ""))
	}

	if endpoint != "" {
		awsConfig = awsConfig.WithEndpoint(endpoint)
	}

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}

	return awsSession, nil
}
