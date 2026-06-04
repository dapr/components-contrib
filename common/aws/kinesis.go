package aws

import (
	"context"
	"errors"
	"fmt"

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
// in the aws.Config into a form that the KCL library can understand. The
// function returns an error if the consumer mode is unsupported or the
// provided AWS configuration is missing required information such as region.
func NewKinesisWorkerConfig(cfg aws.Config, stream, consumer, mode string) (*config.KinesisClientLibConfiguration, error) {
	const sharedMode = "shared"
	if mode != sharedMode {
		return nil, fmt.Errorf("unsupported consumer mode %q", mode)
	}

	region := cfg.Region
	if region == "" {
		return nil, errors.New("region is required for Kinesis worker config")
	}

	if cfg.Credentials != nil {
		return config.NewKinesisClientLibConfigWithCredentials(consumer, stream, region, consumer, cfg.Credentials, cfg.Credentials), nil
	}
	return config.NewKinesisClientLibConfig(consumer, stream, region, consumer), nil
}
