package aws

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
)

// StreamARN fetches the ARN of a Kinesis stream using a v2 client.
func StreamARN(ctx context.Context, client *kinesis.Client, streamName string) (*string, error) {
	if client == nil {
		return nil, errors.New("kinesis client is nil")
	}
	resp, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(streamName)})
	if err != nil {
		return nil, err
	}
	if resp.StreamDescription == nil {
		return nil, errors.New("empty stream description")
	}
	return resp.StreamDescription.StreamARN, nil
}

// NewKinesisWorkerConfig builds a KCL configuration object configured for the
// supplied stream, consumer and region. It converts the credentials contained
// in the aws.Config into a form that the KCL library can understand. If the
// supplied mode is not "shared" or the credential provider is nil, the
// function returns nil.
func NewKinesisWorkerConfig(cfg aws.Config, stream, consumer, mode string) *config.KinesisClientLibConfiguration {
	const sharedMode = "shared"
	if mode != sharedMode {
		return nil
	}

	region := cfg.Region
	if region == "" {
		return nil
	}

	if cfg.Credentials != nil {
		return config.NewKinesisClientLibConfigWithCredentials(consumer, stream, region, consumer, cfg.Credentials, cfg.Credentials)
	}
	return config.NewKinesisClientLibConfig(consumer, stream, region, consumer)
}
