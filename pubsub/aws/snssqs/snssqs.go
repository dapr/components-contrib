package snssqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	sns "github.com/aws/aws-sdk-go/service/sns"
	sqs "github.com/aws/aws-sdk-go/service/sqs"

	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type snsSqs struct {
	// key is the topic name, value is the ARN of the topic.
	topics map[string]string
	// key is the sanitized topic name, value is the actual topic name.
	topicSanitized map[string]string
	// key is the topic name, value holds the ARN of the queue and its url.
	queues        map[string]*sqsQueueInfo
	snsClient     *sns.SNS
	sqsClient     *sqs.SQS
	metadata      *snsSqsMetadata
	logger        logger.Logger
	subscriptions []*string
}

type sqsQueueInfo struct {
	arn string
	url string
}

type snsSqsMetadata struct {
	// name of the queue for this application. The is provided by the runtime as "consumerID".
	sqsQueueName string
	// name of the dead letter queue for this application.
	sqsDeadLettersQueueName string
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

	// amount of time in seconds that a message is hidden from receive requests after it is sent to a subscriber. Default: 10.
	messageVisibilityTimeout int64
	// number of times to resend a message after processing of that message fails before removing that message from the queue. Default: 10.
	messageRetryLimit int64
	// if sqsDeadLettersQueueName is set to a value, then the messageReceiveLimit defines the number of times a message is received
	// before it is moved to the dead-letters queue. This value must be smaller than messageRetryLimit.
	messageReceiveLimit int64
	// amount of time to await receipt of a message before making another request. Default: 1.
	messageWaitTimeSeconds int64
	// maximum number of messages to receive from the queue at a time. Default: 10, Maximum: 10.
	messageMaxNumber int64
}

const (
	awsSqsQueueNameKey = "dapr-queue-name"
	awsSnsTopicNameKey = "dapr-topic-name"
)

// NewSnsSqs - constructor for a new snssqs dapr component.
func NewSnsSqs(l logger.Logger) pubsub.PubSub {
	return &snsSqs{
		logger:        l,
		subscriptions: []*string{},
	}
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

// sanitize topic/queue name to conform with:
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-queues.html
func nameToAWSSanitizedName(name string) string {
	s := []byte(name)

	j := 0
	for _, b := range s {
		if ('a' <= b && b <= 'z') ||
			('A' <= b && b <= 'Z') ||
			('0' <= b && b <= '9') ||
			(b == '-') ||
			(b == '_') {
			s[j] = b
			j++

			if j == 80 {
				break
			}
		}
	}

	return string(s[:j])
}

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
		s.logger.Debugf("AccessKey: %s", val)
		md.AccessKey = val
	}

	if val, ok := getAliasedProperty([]string{"awsSecret", "secretKey"}, metadata); ok {
		s.logger.Debugf("awsToken: %s", val)
		md.SecretKey = val
	}

	if val, ok := getAliasedProperty([]string{"sessionToken"}, metadata); ok {
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

	if val, ok := getAliasedProperty([]string{"sqsDeadLettersQueueName"}, metadata); ok {
		md.sqsDeadLettersQueueName = val
	}

	if val, ok := getAliasedProperty([]string{"messageReceiveLimit"}, metadata); ok {
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

	return &md, nil
}

func (s *snsSqs) Init(metadata pubsub.Metadata) error {
	md, err := s.getSnsSqsMetatdata(metadata)
	if err != nil {
		return err
	}

	s.metadata = md

	// both Publish and Subscribe need reference the topic ARN
	// track these ARNs in this map.
	s.topics = make(map[string]string)
	s.topicSanitized = make(map[string]string)
	s.queues = make(map[string]*sqsQueueInfo)
	sess, err := aws_auth.GetClient(md.AccessKey, md.SecretKey, md.SessionToken, md.Region, md.Endpoint)
	if err != nil {
		return fmt.Errorf("error creating an AWS client: %w", err)
	}
	s.snsClient = sns.New(sess)
	s.sqsClient = sqs.New(sess)

	return nil
}

func (s *snsSqs) createTopic(topic string) (string, string, error) {
	sanitizedName := nameToAWSSanitizedName(topic)
	createTopicResponse, err := s.snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(sanitizedName),
		Tags: []*sns.Tag{{Key: aws.String(awsSnsTopicNameKey), Value: aws.String(topic)}},
	})
	if err != nil {
		return "", "", fmt.Errorf("error while creating an SNS topic: %w", err)
	}

	return *(createTopicResponse.TopicArn), sanitizedName, nil
}

// get the topic ARN from the topics map. If it doesn't exist in the map, try to fetch it from AWS, if it doesn't exist
// at all, issue a request to create the topic.
func (s *snsSqs) getOrCreateTopic(topic string) (string, error) {
	topicArn, ok := s.topics[topic]

	if ok {
		s.logger.Debugf("found existing topic ARN for topic %s: %s", topic, topicArn)

		return topicArn, nil
	}

	s.logger.Debugf("no topic ARN found for %s\n Creating topic instead.", topic)

	topicArn, sanitizedName, err := s.createTopic(topic)
	if err != nil {
		s.logger.Errorf("error creating new topic %s: %v", topic, err)

		return "", err
	}

	// record topic ARN.
	s.topics[topic] = topicArn
	s.topicSanitized[sanitizedName] = topic

	return topicArn, nil
}

func (s *snsSqs) createQueue(queueName string) (*sqsQueueInfo, error) {
	createQueueResponse, err := s.sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(nameToAWSSanitizedName(queueName)),
		Tags:      map[string]*string{awsSqsQueueNameKey: aws.String(queueName)},
	})
	if err != nil {
		return nil, fmt.Errorf("error creaing an SQS queue: %w", err)
	}

	queueAttributesResponse, err := s.sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("QueueArn")},
		QueueUrl:       createQueueResponse.QueueUrl,
	})
	if err != nil {
		s.logger.Errorf("error fetching queue attributes for %s: %v", queueName, err)
	}

	return &sqsQueueInfo{
		arn: *(queueAttributesResponse.Attributes["QueueArn"]),
		url: *(createQueueResponse.QueueUrl),
	}, nil
}

func (s *snsSqs) getOrCreateQueue(queueName string) (*sqsQueueInfo, error) {
	queueArn, ok := s.queues[queueName]

	if ok {
		s.logger.Debugf("Found queue arn for %s: %s", queueName, queueArn)

		return queueArn, nil
	}
	// creating queues is idempotent, the names serve as unique keys among a given region.
	s.logger.Debugf("No queue arn found for %s\nCreating queue", queueName)

	queueInfo, err := s.createQueue(queueName)
	if err != nil {
		s.logger.Errorf("Error creating queue %s: %v", queueName, err)

		return nil, err
	}

	s.queues[queueName] = queueInfo

	return queueInfo, nil
}

func (s *snsSqs) Publish(req *pubsub.PublishRequest) error {
	topicArn, err := s.getOrCreateTopic(req.Topic)
	if err != nil {
		s.logger.Errorf("error getting topic ARN for %s: %v", req.Topic, err)
	}

	message := string(req.Data)
	_, err = s.snsClient.Publish(&sns.PublishInput{
		Message:  &message,
		TopicArn: &topicArn,
	})

	if err != nil {
		wrappedErr := fmt.Errorf("error publishing to topic: %s with topic ARN %s: %w", req.Topic, topicArn, err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	return nil
}

type snsMessage struct {
	Message  string
	TopicArn string
}

func parseTopicArn(arn string) string {
	return arn[strings.LastIndex(arn, ":")+1:]
}

func (s *snsSqs) acknowledgeMessage(queueURL string, receiptHandle *string) error {
	if _, err := s.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: receiptHandle,
	}); err != nil {
		return fmt.Errorf("error deleting SQS message: %w", err)
	}

	return nil
}

func (s *snsSqs) handleMessage(message *sqs.Message, queueInfo, deadLettersQueueInfo *sqsQueueInfo, handler pubsub.Handler) error {
	// if this message has been received > x times, delete from queue, it's borked.
	recvCount, ok := message.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]

	if !ok {
		return fmt.Errorf(
			"no ApproximateReceiveCount returned with response, will not attempt further processing: %v", message)
	}

	recvCountInt, err := strconv.ParseInt(*recvCount, 10, 32)
	if err != nil {
		return fmt.Errorf("error parsing ApproximateReceiveCount from message: %v", message)
	}

	// if we are over the allowable retry limit, and there is no dead-letters queue, delete the message from the queue.
	if deadLettersQueueInfo == nil && recvCountInt >= s.metadata.messageRetryLimit {
		if innerErr := s.acknowledgeMessage(queueInfo.url, message.ReceiptHandle); innerErr != nil {
			return fmt.Errorf("error acknowledging message after receiving the message too many times: %w", innerErr)
		}

		return fmt.Errorf(
			"message received greater than %v times, deleting this message without further processing", s.metadata.messageRetryLimit)
	}
	// ... else, there is no need to actively do something if we reached the limit defined in messageReceiveLimit as the message had
	// already been moved to the dead-letters queue by SQS.
	if deadLettersQueueInfo != nil && recvCountInt >= s.metadata.messageReceiveLimit {
		s.logger.Warnf(
			"message received greater than %v times, moving this message without further processing to dead-letters queue: %v", s.metadata.messageReceiveLimit, s.metadata.sqsDeadLettersQueueName)
	}

	// otherwise try to handle the message.
	var messageBody snsMessage
	err = json.Unmarshal([]byte(*(message.Body)), &messageBody)

	if err != nil {
		return fmt.Errorf("error unmarshalling message: %w", err)
	}

	topic := parseTopicArn(messageBody.TopicArn)
	topic = s.topicSanitized[topic]
	err = handler(context.Background(), &pubsub.NewMessage{
		Data:  []byte(messageBody.Message),
		Topic: topic,
	})

	if err != nil {
		return fmt.Errorf("error handling message: %w", err)
	}

	// otherwise, there was no error, acknowledge the message.
	return s.acknowledgeMessage(queueInfo.url, message.ReceiptHandle)
}

func (s *snsSqs) consumeSubscription(queueInfo, deadLettersQueueInfo *sqsQueueInfo, handler pubsub.Handler) {
	go func() {
		for {
			messageResponse, err := s.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
				// use this property to decide when a message should be discarded.
				AttributeNames: []*string{
					aws.String(sqs.MessageSystemAttributeNameApproximateReceiveCount),
				},
				MaxNumberOfMessages: aws.Int64(s.metadata.messageMaxNumber),
				QueueUrl:            &queueInfo.url,
				VisibilityTimeout:   aws.Int64(s.metadata.messageVisibilityTimeout),
				WaitTimeSeconds:     aws.Int64(s.metadata.messageWaitTimeSeconds),
			})
			if err != nil {
				s.logger.Errorf("error consuming topic: %v", err)

				continue
			}

			// retry receiving messages.
			if len(messageResponse.Messages) < 1 {
				s.logger.Debug("No messages received, requesting again")

				continue
			}

			s.logger.Debugf("%v message(s) received", len(messageResponse.Messages))

			for _, m := range messageResponse.Messages {
				if err := s.handleMessage(m, queueInfo, deadLettersQueueInfo, handler); err != nil {
					s.logger.Error(err)
				}
			}
		}
	}()
}

func (s *snsSqs) createDeadLettersQueue() (*sqsQueueInfo, error) {
	var deadLettersQueueInfo *sqsQueueInfo
	deadLettersQueueInfo, err := s.getOrCreateQueue(s.metadata.sqsDeadLettersQueueName)
	if err != nil {
		s.logger.Errorf("error retrieving SQS dead-letter queue: %w", err)

		return nil, err
	}

	return deadLettersQueueInfo, nil
}

func (s *snsSqs) createQueueAttributesWithDeadLetters(queueInfo, deadLettersQueueInfo *sqsQueueInfo) (*sqs.SetQueueAttributesInput, error) {
	policy := map[string]string{
		"deadLetterTargetArn": deadLettersQueueInfo.arn,
		"maxReceiveCount":     strconv.FormatInt(s.metadata.messageReceiveLimit, 10),
	}

	b, err := json.Marshal(policy)
	if err != nil {
		wrappedErr := fmt.Errorf("error marshalling dead-letters queue policy: %w", err)
		s.logger.Error(wrappedErr)

		return nil, wrappedErr
	}

	sqsSetQueueAttributesInput := &sqs.SetQueueAttributesInput{
		QueueUrl: &queueInfo.url,
		Attributes: map[string]*string{
			sqs.QueueAttributeNameRedrivePolicy: aws.String(string(b)),
		},
	}

	return sqsSetQueueAttributesInput, nil
}

func (s *snsSqs) restrictQueuePublishPolicyToOnlySNS(sqsQueueInfo *sqsQueueInfo, snsARN string) error {
	// only permit SNS to send messages to SQS using the created subscription.
	getQueueAttributesOutput, err := s.sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{QueueUrl: &sqsQueueInfo.url, AttributeNames: []*string{aws.String(sqs.QueueAttributeNamePolicy)}})
	if err != nil {
		return fmt.Errorf("error getting queue attributes: %w", err)
	}

	newStatement := &statement{
		Effect:    "Allow",
		Principal: principal{Service: "sns.amazonaws.com"},
		Action:    "sqs:SendMessage",
		Resource:  sqsQueueInfo.arn,
		Condition: condition{
			ArnEquals: arnEquals{
				AwsSourceArn: snsARN,
			},
		},
	}

	policy := &policy{Version: "2012-10-17"}
	if policyStr, ok := getQueueAttributesOutput.Attributes[sqs.QueueAttributeNamePolicy]; ok {
		// look for the current statement if exists, else add it and store.
		if err = json.Unmarshal([]byte(*policyStr), policy); err != nil {
			return fmt.Errorf("error unmarshalling sqs policy: %w", err)
		}
		if policy.statementExists(newStatement) {
			// nothing to do.
			return nil
		}
	}

	policy.addStatement(newStatement)
	b, uerr := json.Marshal(policy)
	if uerr != nil {
		return fmt.Errorf("failed serializing new sqs policy: %w", uerr)
	}

	if _, err = s.sqsClient.SetQueueAttributes(&(sqs.SetQueueAttributesInput{
		Attributes: map[string]*string{
			"Policy": aws.String(string(b)),
		},
		QueueUrl: &sqsQueueInfo.url,
	})); err != nil {
		return fmt.Errorf("error setting queue subscription policy: %w", err)
	}

	return nil
}

func (s *snsSqs) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	// subscribers declare a topic ARN and declare a SQS queue to use
	// these should be idempotent - queues should not be created if they exist.
	topicArn, err := s.getOrCreateTopic(req.Topic)
	if err != nil {
		s.logger.Errorf("error getting topic ARN for %s: %w", req.Topic, err)

		return err
	}

	// this is the ID of the application, it is supplied via runtime as "consumerID".
	var queueInfo *sqsQueueInfo
	queueInfo, err = s.getOrCreateQueue(s.metadata.sqsQueueName)
	if err != nil {
		s.logger.Errorf("error retrieving SQS queue: %w", err)

		return err
	}

	// only after a SQS queue and SNS topic had been setup, we restrict the SendMessage action to SNS as sole source
	// to prevent anyone but SNS to publish message to SQS.
	err = s.restrictQueuePublishPolicyToOnlySNS(queueInfo, topicArn)
	if err != nil {
		s.logger.Errorf("error setting sns-sqs subscription policy: %w", err)

		return err
	}

	// apply the dead letters queue attributes to the current queue.
	var deadLettersQueueInfo *sqsQueueInfo
	if len(s.metadata.sqsDeadLettersQueueName) > 0 {
		var derr error
		deadLettersQueueInfo, derr = s.createDeadLettersQueue()
		if derr != nil {
			s.logger.Errorf("error creating dead-letter queue: %w", derr)

			return derr
		}

		var sqsSetQueueAttributesInput *sqs.SetQueueAttributesInput
		sqsSetQueueAttributesInput, derr = s.createQueueAttributesWithDeadLetters(queueInfo, deadLettersQueueInfo)
		if derr != nil {
			s.logger.Errorf("error creatubg queue attributes for dead-letter queue: %w", derr)

			return derr
		}
		_, derr = s.sqsClient.SetQueueAttributes(sqsSetQueueAttributesInput)
		if derr != nil {
			wrappedErr := fmt.Errorf("error updating queue attributes with dead-letter queue: %w", derr)
			s.logger.Error(wrappedErr)

			return wrappedErr
		}
	}

	// subscription creation is idempotent. Subscriptions are unique by topic/queue.
	subscribeOutput, err := s.snsClient.Subscribe(&sns.SubscribeInput{
		Attributes:            nil,
		Endpoint:              &queueInfo.arn, // create SQS queue per subscription.
		Protocol:              aws.String("sqs"),
		ReturnSubscriptionArn: nil,
		TopicArn:              &topicArn,
	})
	if err != nil {
		wrappedErr := fmt.Errorf("error subscribing to topic %s: %w", req.Topic, err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	s.subscriptions = append(s.subscriptions, subscribeOutput.SubscriptionArn)
	s.logger.Debugf("Subscribed to topic %s: %v", req.Topic, subscribeOutput)

	s.consumeSubscription(queueInfo, deadLettersQueueInfo, handler)

	return nil
}

func (s *snsSqs) Close() error {
	return nil
}

func (s *snsSqs) Features() []pubsub.Feature {
	return nil
}
