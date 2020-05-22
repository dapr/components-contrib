package aws_authentication

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

func GetClient(accessKey string, secretKey string, region string) (*session.Session, error) {
	awsConfig := aws.NewConfig()

	if region != "" {
		awsConfig = awsConfig.WithRegion(region)
	}
	if accessKey != "" && secretKey != "" {
		awsConfig = awsConfig.WithCredentials(credentials.NewStaticCredentials(accessKey, secretKey, ""))
	}

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}
	return awsSession, nil
}