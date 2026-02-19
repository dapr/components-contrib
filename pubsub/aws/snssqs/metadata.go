package snssqs

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/metadata"
)

type snsSqsMetadata struct {
	// Ignored by metadata parser because included in built-in authentication profile
	// access key to use for accessing sqs/sns.
	AccessKey string `json:"accessKey" mapstructure:"accessKey" mdignore:"true"`
	// secret key to use for accessing sqs/sns.
	SecretKey string `json:"secretKey" mapstructure:"secretKey" mdignore:"true"`
	// aws session token to use.
	SessionToken string `mapstructure:"sessionToken" mdignore:"true"`

	// aws endpoint for the component to use.
	Endpoint string `mapstructure:"endpoint"`
	// aws region in which SNS/SQS should create resources.
	// TODO: rm the alias on region in Dapr 1.17.
	Region string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion" mdignore:"true"`
	// aws partition in which SNS/SQS should create resources.
	Partition string `mapstructure:"partition" mdignore:"true"`
	// name of the queue for this application. The is provided by the runtime as "consumerID".
	SqsQueueName string `mapstructure:"consumerID" mdignore:"true"`
	// name of the dead letter queue for this application.
	SqsDeadLettersQueueName string `mapstructure:"sqsDeadLettersQueueName"`
	// flag to SNS and SQS FIFO.
	Fifo bool `mapstructure:"fifo"`
	// a namespace for SNS SQS FIFO to order messages within that group. limits consumer concurrency if set but guarantees that all
	// published messages would be ordered by their arrival time to SQS.
	// see: https://aws.amazon.com/blogs/compute/solving-complex-ordering-challenges-with-amazon-sqs-fifo-queues/
	FifoMessageGroupID string `mapstructure:"fifoMessageGroupID"`
	// amount of time in seconds that a message is hidden from receive requests after it is sent to a subscriber. Default: 10.
	MessageVisibilityTimeout int32 `mapstructure:"messageVisibilityTimeout"`
	// number of times to resend a message after processing of that message fails before removing that message from the queue. Default: 10.
	MessageRetryLimit int64 `mapstructure:"messageRetryLimit"`
	// upon reaching the messageRetryLimit, disables the default deletion behaviour of the message from the SQS queue, and resetting the message visibilty on SQS
	// so that other consumers can try consuming that message.
	DisableDeleteOnRetryLimit bool `mapstructure:"disableDeleteOnRetryLimit"`
	// if sqsDeadLettersQueueName is set to a value, then the MessageReceiveLimit defines the number of times a message is received
	// before it is moved to the dead-letters queue. This value must be smaller than messageRetryLimit.
	MessageReceiveLimit int64 `mapstructure:"messageReceiveLimit"`
	// amount of time to await receipt of a message before making another request. Default: 2.
	MessageWaitTimeSeconds int32 `mapstructure:"messageWaitTimeSeconds"`
	// maximum number of messages to receive from the queue at a time. Default: 10, Maximum: 10.
	MessageMaxNumber int32 `mapstructure:"messageMaxNumber"`
	// disable resource provisioning of SNS and SQS.
	DisableEntityManagement bool `mapstructure:"disableEntityManagement"`
	// assets creation timeout.
	AssetsManagementTimeoutSeconds float64 `mapstructure:"assetsManagementTimeoutSeconds"`
	// aws account ID. internally resolved if not given.
	AccountID string `mapstructure:"accountID"`
	// processing concurrency mode
	ConcurrencyMode pubsub.ConcurrencyMode `mapstructure:"concurrencyMode"`
	// limits the number of concurrent goroutines
	ConcurrencyLimit int `mapstructure:"concurrencyLimit"`
}

func maskLeft(s string) string {
	rs := []rune(s)
	for i := range len(rs) - 4 {
		rs[i] = 'X'
	}
	return string(rs)
}

// getPartitionFromRegion returns the AWS partition for a given region.
// TODO: @mikeee - remove this partition acquisition.
func getPartitionFromRegion(region string) string {
	switch {
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(region, "eusc-"):
		return "aws-eusc"
	case strings.HasPrefix(region, "us-iso-"):
		return "aws-iso"
	case strings.HasPrefix(region, "us-isob-"):
		return "aws-iso-b"
	case strings.HasPrefix(region, "eu-isoe-"):
		return "aws-iso-e"
	case strings.HasPrefix(region, "us-isof-"):
		return "aws-iso-f"
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	default:
		return "aws"
	}
}

func (s *snsSqs) getSnsSqsMetadata(meta pubsub.Metadata) (*snsSqsMetadata, error) {
	md := &snsSqsMetadata{
		AssetsManagementTimeoutSeconds: assetsManagementDefaultTimeoutSeconds,
		MessageVisibilityTimeout:       10,
		MessageRetryLimit:              10,
		MessageWaitTimeSeconds:         2,
		MessageMaxNumber:               10,
	}
	upgradeMetadata(&meta)
	err := metadata.DecodeMetadata(meta.Properties, md)
	if err != nil {
		return nil, err
	}

	if md.Region != "" {
		// Use an explicitly provided partition if available.
		if md.Partition == "" {
			md.Partition = getPartitionFromRegion(md.Region)
		}
	}

	if md.SqsQueueName == "" {
		return nil, errors.New("consumerID must be set")
	}

	if md.MessageVisibilityTimeout < 1 {
		return nil, errors.New("messageVisibilityTimeout must be greater than 0")
	}

	if md.MessageRetryLimit < 2 {
		return nil, errors.New("messageRetryLimit must be greater than 1")
	}

	// XOR on having either a valid messageReceiveLimit and invalid sqsDeadLettersQueueName, and vice versa.
	if (md.MessageReceiveLimit > 0 || len(md.SqsDeadLettersQueueName) > 0) && !(md.MessageReceiveLimit > 0 && len(md.SqsDeadLettersQueueName) > 0) {
		return nil, errors.New("to use SQS dead letters queue, messageReceiveLimit and sqsDeadLettersQueueName must both be set to a value")
	}

	if len(md.SqsDeadLettersQueueName) > 0 && md.DisableDeleteOnRetryLimit {
		return nil, errors.New("configuration conflict: 'disableDeleteOnRetryLimit' cannot be set to 'true' when 'sqsDeadLettersQueueName' is set to a value. either remove this configuration or set 'disableDeleteOnRetryLimit' to 'false'")
	}

	if md.MessageWaitTimeSeconds < 1 {
		return nil, errors.New("messageWaitTimeSeconds must be greater than 0")
	}

	// fifo settings: assign user provided Message Group ID
	// for more details, see: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html
	if md.FifoMessageGroupID == "" {
		md.FifoMessageGroupID = meta.Properties[pubsub.RuntimeConsumerIDKey]
	}

	if md.MessageMaxNumber < 1 {
		return nil, errors.New("messageMaxNumber must be greater than 0")
	} else if md.MessageMaxNumber > 10 {
		return nil, errors.New("messageMaxNumber must be less than or equal to 10")
	}

	if err := md.setConcurrencyMode(meta.Properties); err != nil {
		return nil, err
	}

	if md.ConcurrencyLimit < 0 {
		return nil, errors.New("concurrencyLimit must be greater than or equal to 0")
	}

	s.logger.Debug(md.hideDebugPrintedCredentials())

	return md, nil
}

func (md *snsSqsMetadata) setConcurrencyMode(props map[string]string) error {
	c, err := pubsub.Concurrency(props)
	if err != nil {
		return err
	}
	md.ConcurrencyMode = c

	return nil
}

func (md *snsSqsMetadata) hideDebugPrintedCredentials() string {
	mdCopy := *md
	mdCopy.AccessKey = maskLeft(md.AccessKey)
	mdCopy.SecretKey = maskLeft(md.SecretKey)
	mdCopy.SessionToken = maskLeft(md.SessionToken)

	return fmt.Sprintf("%#v\n", mdCopy)
}

func upgradeMetadata(m *pubsub.Metadata) {
	upgradeMap := map[string]string{
		"Endpoint":     "endpoint",
		"awsAccountID": "accessKey",
		"awsSecret":    "secretKey",
		"awsRegion":    "region",
	}

	for oldKey, newKey := range upgradeMap {
		if val, ok := m.Properties[oldKey]; ok {
			m.Properties[newKey] = val
			delete(m.Properties, oldKey)
		}
	}
}
