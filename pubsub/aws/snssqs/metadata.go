package snssqs

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/dapr/components-contrib/pubsub"
)

type snsSqsMetadata struct {
	// aws endpoint for the component to use.
	Endpoint string
	// access key to use for accessing sqs/sns.
	AccessKey string
	// secret key to use for accessing sqs/sns.
	SecretKey string
	// aws session token to use.
	SessionToken string
	// aws region in which SNS/SQS should create resources.
	Region string
	// name of the queue for this application. The is provided by the runtime as "consumerID".
	sqsQueueName string
	// name of the dead letter queue for this application.
	sqsDeadLettersQueueName string
	// flag to SNS and SQS FIFO.
	fifo bool
	// a namespace for SNS SQS FIFO to order messages within that group. limits consumer concurrency if set but guarantees that all
	// published messages would be ordered by their arrival time to SQS.
	// see: https://aws.amazon.com/blogs/compute/solving-complex-ordering-challenges-with-amazon-sqs-fifo-queues/
	fifoMessageGroupID string
	// amount of time in seconds that a message is hidden from receive requests after it is sent to a subscriber. Default: 10.
	messageVisibilityTimeout int64
	// number of times to resend a message after processing of that message fails before removing that message from the queue. Default: 10.
	messageRetryLimit int64
	// upon reaching the messageRetryLimit, disables the default deletion behaviour of the message from the SQS queue, and resetting the message visibilty on SQS
	// so that other consumers can try consuming that message.
	disableDeleteOnRetryLimit bool
	// if sqsDeadLettersQueueName is set to a value, then the messageReceiveLimit defines the number of times a message is received
	// before it is moved to the dead-letters queue. This value must be smaller than messageRetryLimit.
	messageReceiveLimit int64
	// amount of time to await receipt of a message before making another request. Default: 1.
	messageWaitTimeSeconds int64
	// maximum number of messages to receive from the queue at a time. Default: 10, Maximum: 10.
	messageMaxNumber int64
	// disable resource provisioning of SNS and SQS.
	disableEntityManagement bool
	// assets creation timeout.
	assetsManagementTimeoutSeconds float64
	// aws account ID. internally resolved if not given.
	accountID string
}

func getAliasedProperty(aliases []string, metadata pubsub.Metadata) (string, bool) {
	props := metadata.Properties
	for _, s := range aliases {
		if val, ok := props[s]; ok {
			return val, true
		}
	}

	return "", false
}

func parseInt64(input string, propertyName string) (int64, error) {
	number, err := strconv.Atoi(input)
	if err != nil {
		return -1, fmt.Errorf("parsing %s failed with: %w", propertyName, err)
	}

	return int64(number), nil
}

func parseBool(input string, propertyName string) (bool, error) {
	val, err := strconv.ParseBool(input)
	if err != nil {
		return false, fmt.Errorf("parsing %s failed with: %w", propertyName, err)
	}
	return val, nil
}

func parseFloat64(input string, propertyName string) (float64, error) {
	val, err := strconv.ParseFloat(input, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s failed with: %w", propertyName, err)
	}
	return val, nil
}

//nolint:cyclop
func (s *snsSqs) getSnsSqsMetatdata(metadata pubsub.Metadata) (*snsSqsMetadata, error) {
	md := snsSqsMetadata{}
	props := metadata.Properties
	md.sqsQueueName = metadata.Properties["consumerID"]
	s.logger.Debugf("Setting queue name to %s", md.sqsQueueName)

	if val, ok := getAliasedProperty([]string{"Endpoint", "endpoint"}, metadata); ok {
		s.logger.Debugf("endpoint: %s", val)
		md.Endpoint = val
	}

	if val, ok := getAliasedProperty([]string{"awsAccountID", "accessKey"}, metadata); ok {
		s.logger.Debugf("accessKey: %s", val)
		md.AccessKey = val
	}

	if val, ok := getAliasedProperty([]string{"awsSecret", "secretKey"}, metadata); ok {
		s.logger.Debugf("secretKey: %s", val)
		md.SecretKey = val
	}

	if val, ok := props["sessionToken"]; ok {
		md.SessionToken = val
	}

	if val, ok := getAliasedProperty([]string{"awsRegion", "region"}, metadata); ok {
		md.Region = val
	}

	if val, ok := props["messageVisibilityTimeout"]; !ok {
		md.messageVisibilityTimeout = 10
	} else {
		timeout, err := parseInt64(val, "messageVisibilityTimeout")
		if err != nil {
			return nil, err
		}

		if timeout < 1 {
			return nil, errors.New("messageVisibilityTimeout must be greater than 0")
		}

		md.messageVisibilityTimeout = timeout
	}

	if val, ok := props["messageRetryLimit"]; !ok {
		md.messageRetryLimit = 10
	} else {
		retryLimit, err := parseInt64(val, "messageRetryLimit")
		if err != nil {
			return nil, err
		}

		if retryLimit < 2 {
			return nil, errors.New("messageRetryLimit must be greater than 1")
		}

		md.messageRetryLimit = retryLimit
	}

	if val, ok := props["sqsDeadLettersQueueName"]; ok {
		md.sqsDeadLettersQueueName = val
	}

	if val, ok := props["messageReceiveLimit"]; ok {
		messageReceiveLimit, err := parseInt64(val, "messageReceiveLimit")
		if err != nil {
			return nil, err
		}
		// assign: used provided configuration
		md.messageReceiveLimit = messageReceiveLimit
	}

	// XOR on having either a valid messageReceiveLimit and invalid sqsDeadLettersQueueName, and vice versa.
	if (md.messageReceiveLimit > 0 || len(md.sqsDeadLettersQueueName) > 0) && !(md.messageReceiveLimit > 0 && len(md.sqsDeadLettersQueueName) > 0) {
		return nil, errors.New("to use SQS dead letters queue, messageReceiveLimit and sqsDeadLettersQueueName must both be set to a value")
	}

	if val, ok := props["disableDeleteOnRetryLimit"]; ok {
		disableDeleteOnRetryLimit, err := parseBool(val, "disableDeleteOnRetryLimit")
		if err != nil {
			return nil, err
		}

		if len(md.sqsDeadLettersQueueName) > 0 && disableDeleteOnRetryLimit {
			return nil, errors.New("configuration conflict: 'disableDeleteOnRetryLimit' cannot be set to 'true' when 'sqsDeadLettersQueueName' is set to a value. either remove this configuration or set 'disableDeleteOnRetryLimit' to 'false'")
		}

		md.disableDeleteOnRetryLimit = disableDeleteOnRetryLimit
	} else {
		// default when not configured.
		md.disableDeleteOnRetryLimit = false
	}

	// fifo settings: enable/disable SNS and SQS FIFO.
	if val, ok := props["fifo"]; ok {
		fifo, err := parseBool(val, "fifo")
		if err != nil {
			return nil, err
		}
		md.fifo = fifo
	} else {
		md.fifo = false
	}

	// fifo settings: assign user provided Message Group ID
	// for more details, see: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html
	if val, ok := props["fifoMessageGroupID"]; ok {
		md.fifoMessageGroupID = val
	}

	if val, ok := props["messageWaitTimeSeconds"]; !ok {
		md.messageWaitTimeSeconds = 1
	} else {
		waitTime, err := parseInt64(val, "messageWaitTimeSeconds")
		if err != nil {
			return nil, err
		}

		if waitTime < 1 {
			return nil, errors.New("messageWaitTimeSeconds must be greater than 0")
		}

		md.messageWaitTimeSeconds = waitTime
	}

	if val, ok := props["messageMaxNumber"]; !ok {
		md.messageMaxNumber = 10
	} else {
		maxNumber, err := parseInt64(val, "messageMaxNumber")
		if err != nil {
			return nil, err
		}

		if maxNumber < 1 {
			return nil, errors.New("messageMaxNumber must be greater than 0")
		} else if maxNumber > 10 {
			return nil, errors.New("messageMaxNumber must be less than or equal to 10")
		}

		md.messageMaxNumber = maxNumber
	}

	if val, ok := props["disableEntityManagement"]; ok {
		parsed, err := parseBool(val, "disableEntityManagement")
		if err != nil {
			return nil, err
		}
		md.disableEntityManagement = parsed
	}

	if val, ok := props["assetsManagementTimeoutSeconds"]; ok {
		parsed, err := parseFloat64(val, "assetsManagementTimeoutSeconds")
		if err != nil {
			return nil, err
		}
		md.assetsManagementTimeoutSeconds = parsed
	} else {
		md.assetsManagementTimeoutSeconds = assetsManagementDefaultTimeoutSeconds
	}

	return &md, nil
}
