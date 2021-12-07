package snssqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"

	gonanoid "github.com/matoous/go-nanoid/v2"

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
	queues map[string]*sqsQueueInfo
	// key is a composite key of queue ARN and topic ARN mapping to subscription ARN.
	subscriptions map[string]string
	snsClient     *sns.SNS
	sqsClient     *sqs.SQS
	stsClient     *sts.STS
	metadata      *snsSqsMetadata
	logger        logger.Logger
	id            string
}

type sqsQueueInfo struct {
	arn string
	url string
}

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
	// if sqsDeadLettersQueueName is set to a value, then the messageReceiveLimit defines the number of times a message is received
	// before it is moved to the dead-letters queue. This value must be smaller than messageRetryLimit.
	messageReceiveLimit int64
	// amount of time to await receipt of a message before making another request. Default: 1.
	messageWaitTimeSeconds int64
	// maximum number of messages to receive from the queue at a time. Default: 10, Maximum: 10.
	messageMaxNumber int64
	// disable resource provisioning of SNS and SQS.
	disableEntityManagement bool
	// aws account ID.
	accountID string
}

const (
	awsSqsQueueNameKey = "dapr-queue-name"
	awsSnsTopicNameKey = "dapr-topic-name"
	awsSqsFifoSuffix   = ".fifo"
	maxAWSNameLength   = 80
)

// NewSnsSqs - constructor for a new snssqs dapr component.
func NewSnsSqs(l logger.Logger) pubsub.PubSub {
	id, err := gonanoid.New()
	if err != nil {
		l.Fatalf("failed generating unique nano id: %s", err)
	}

	return &snsSqs{
		logger: l,
		id:     id,
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

func parseBool(input string, propertyName string) (bool, error) {
	val, err := strconv.ParseBool(input)
	if err != nil {
		return false, fmt.Errorf("parsing %s failed with: %w", propertyName, err)
	}
	return val, nil
}

// sanitize topic/queue name to conform with:
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-queues.html
func nameToAWSSanitizedName(name string, isFifo bool) string {
	// first remove suffix if exists, and user requested a FIFO name, then sanitize the passed in name.
	hasFifoSuffix := false
	if strings.HasSuffix(name, awsSqsFifoSuffix) && isFifo {
		hasFifoSuffix = true
		name = name[:len(name)-len(awsSqsFifoSuffix)]
	}

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

			if j == maxAWSNameLength {
				break
			}
		}
	}

	// reattach/add the suffix to the sanitized name, trim more if adding the suffix would exceed the maxLength.
	if hasFifoSuffix || isFifo {
		delta := j + len(awsSqsFifoSuffix) - maxAWSNameLength
		if delta > 0 {
			j -= delta
		}
		return string(s[:j]) + awsSqsFifoSuffix
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

	return &md, nil
}

func (s *snsSqs) Init(metadata pubsub.Metadata) error {
	md, err := s.getSnsSqsMetatdata(metadata)
	if err != nil {
		return err
	}

	s.metadata = md

	// both Publish and Subscribe need reference the topic ARN, queue ARN and subscription ARN between topic and queue
	// track these ARNs in these maps.
	s.topics = make(map[string]string)
	s.topicSanitized = make(map[string]string)
	s.queues = make(map[string]*sqsQueueInfo)
	s.subscriptions = make(map[string]string)

	sess, err := aws_auth.GetClient(md.AccessKey, md.SecretKey, md.SessionToken, md.Region, md.Endpoint)
	if err != nil {
		return fmt.Errorf("error creating an AWS client: %w", err)
	}

	s.stsClient = sts.New(sess)
	callerIDOutput, err := s.stsClient.GetCallerIdentity(&sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("error fetching sts caller ID: %w", err)
	}

	s.metadata.accountID = *callerIDOutput.Account

	s.snsClient = sns.New(sess)
	s.sqsClient = sqs.New(sess)

	return nil
}

func (s *snsSqs) buildARN(serviceName, entityName string) string {
	// arn:aws:sns:us-east-1:302212680347:aws-controltower-SecurityNotifications
	return fmt.Sprintf("arn:aws:%s:%s:%s:%s", serviceName, s.metadata.Region, s.metadata.accountID, entityName)
}

func (s *snsSqs) createTopic(topic string) (string, error) {
	sanitizedName := nameToAWSSanitizedName(topic, s.metadata.fifo)
	snsCreateTopicInput := &sns.CreateTopicInput{
		Name: aws.String(sanitizedName),
		Tags: []*sns.Tag{{Key: aws.String(awsSnsTopicNameKey), Value: aws.String(topic)}},
	}

	if s.metadata.fifo {
		attributes := map[string]*string{"FifoTopic": aws.String("true"), "ContentBasedDeduplication": aws.String("true")}
		snsCreateTopicInput.SetAttributes(attributes)
	}

	createTopicResponse, err := s.snsClient.CreateTopic(snsCreateTopicInput)
	if err != nil {
		return "", fmt.Errorf("error while creating an SNS topic: %w", err)
	}

	return *(createTopicResponse.TopicArn), nil
}

func (s *snsSqs) getTopicArn(topic string) (string, error) {
	arn := s.buildARN("sns", topic)
	getTopicOutput, err := s.snsClient.GetTopicAttributes(&sns.GetTopicAttributesInput{TopicArn: aws.String(arn)})
	if err != nil {
		return "", fmt.Errorf("error: %w while getting topic: %v with arn: %v", err, topic, arn)
	}

	return *getTopicOutput.Attributes["TopicArn"], nil
}

// get the topic ARN from the topics map. If it doesn't exist in the map, try to fetch it from AWS, if it doesn't exist
// at all, issue a request to create the topic.
func (s *snsSqs) getOrCreateTopic(topic string) (string, error) {
	var (
		err      error
		topicArn string
		ok       bool
	)

	topicArn, ok = s.topics[topic]
	if ok {
		s.logger.Debugf("found existing topic ARN for topic %s: %s", topic, topicArn)

		return topicArn, nil
	}

	sanitizedName := nameToAWSSanitizedName(topic, s.metadata.fifo)
	if !s.metadata.disableEntityManagement {
		topicArn, err = s.createTopic(sanitizedName)
		if err != nil {
			s.logger.Errorf("error creating new topic %s: %w", topic, err)

			return "", err
		}
	} else {
		topicArn, err = s.getTopicArn(sanitizedName)
		if err != nil {
			s.logger.Errorf("error fetching info for topic %s: %w", topic, err)

			return "", err
		}
	}

	// record topic ARN.
	s.topics[topic] = topicArn
	s.topicSanitized[sanitizedName] = topic

	return topicArn, nil
}

func (s *snsSqs) createQueue(queueName string) (*sqsQueueInfo, error) {
	sanitizedName := nameToAWSSanitizedName(queueName, s.metadata.fifo)
	sqsCreateQueueInput := &sqs.CreateQueueInput{
		QueueName: aws.String(sanitizedName),
		Tags:      map[string]*string{awsSqsQueueNameKey: aws.String(queueName)},
	}

	if s.metadata.fifo {
		attributes := map[string]*string{"FifoQueue": aws.String("true"), "ContentBasedDeduplication": aws.String("true")}
		sqsCreateQueueInput.SetAttributes(attributes)
	}

	createQueueResponse, err := s.sqsClient.CreateQueue(sqsCreateQueueInput)
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

func (s *snsSqs) getQueueArn(queueName string) (*sqsQueueInfo, error) {
	queueURLOutput, err := s.sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(queueName), QueueOwnerAWSAccountId: aws.String(s.metadata.accountID)})
	if err != nil {
		return nil, fmt.Errorf("error: %w while getting url of queue: %s", err, queueName)
	}
	url := queueURLOutput.QueueUrl

	var getQueueOutput *sqs.GetQueueAttributesOutput
	getQueueOutput, err = s.sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{QueueUrl: url, AttributeNames: []*string{aws.String("QueueArn")}})
	if err != nil {
		return nil, fmt.Errorf("error: %w while getting information for queue: %s, with url: %s", err, queueName, *url)
	}

	return &sqsQueueInfo{arn: *getQueueOutput.Attributes["QueueArn"], url: *url}, nil
}

func (s *snsSqs) getOrCreateQueue(queueName string) (*sqsQueueInfo, error) {
	var (
		err       error
		queueInfo *sqsQueueInfo
		ok        bool
	)

	queueInfo, ok = s.queues[queueName]
	if ok {
		s.logger.Debugf("Found queue arn for %s: %s", queueName, queueInfo.arn)

		return queueInfo, nil
	}
	// creating queues is idempotent, the names serve as unique keys among a given region.
	s.logger.Debugf("No queue arn found for %s\nCreating queue", queueName)

	sanitizedName := nameToAWSSanitizedName(queueName, s.metadata.fifo)

	if !s.metadata.disableEntityManagement {
		queueInfo, err = s.createQueue(sanitizedName)
		if err != nil {
			s.logger.Errorf("Error creating queue %s: %v", queueName, err)

			return nil, err
		}
	} else {
		queueInfo, err = s.getQueueArn(sanitizedName)
		if err != nil {
			s.logger.Errorf("error fetching info for queue %s: %w", queueName, err)

			return nil, err
		}
	}

	s.queues[queueName] = queueInfo

	return queueInfo, nil
}

func (s *snsSqs) getMessageGroupID(req *pubsub.PublishRequest) *string {
	if len(s.metadata.fifoMessageGroupID) > 0 {
		return &s.metadata.fifoMessageGroupID
	}
	// each daprd, of a given PubSub, of a given publisher application publishes to a message group ID of its own.
	// for example: for a daprd serving the SNS/SQS Pubsub component we generate a unique id -> A; that component serves on behalf
	// of a given PubSub deployment name B, and component A publishes to SNS on behalf of a dapr application named C (effectively to topic C).
	// therefore the created message group ID for publishing messages in the aforementioned setup is "A:B:C".
	fifoMessageGroupID := fmt.Sprintf("%s:%s:%s", s.id, req.PubsubName, req.Topic)
	return &fifoMessageGroupID
}

func (s *snsSqs) createSNSSQSSubscription(queueArn, topicArn string) (string, error) {
	subscribeOutput, err := s.snsClient.Subscribe(&sns.SubscribeInput{
		Attributes:            nil,
		Endpoint:              aws.String(queueArn), // create SQS queue per subscription.
		Protocol:              aws.String("sqs"),
		ReturnSubscriptionArn: nil,
		TopicArn:              aws.String(topicArn),
	})
	if err != nil {
		wrappedErr := fmt.Errorf("error subscribing to sns topic arn: %s, to queue arn: %s %w", topicArn, queueArn, err)
		s.logger.Error(wrappedErr)

		return "", wrappedErr
	}

	return *subscribeOutput.SubscriptionArn, nil
}

func (s *snsSqs) getSNSSQSSubscriptionArn(topicArn string) (string, error) {
	listSubscriptionsOutput, err := s.snsClient.ListSubscriptionsByTopic(&sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(topicArn)})
	if err != nil {
		return "", fmt.Errorf("error listing subsriptions for topic arn: %v: %w", topicArn, err)
	}

	for _, subscription := range listSubscriptionsOutput.Subscriptions {
		if *subscription.TopicArn == topicArn {
			return *subscription.SubscriptionArn, nil
		}
	}

	return "", fmt.Errorf("sns sqs subscription not found for topic arn")
}

func (s *snsSqs) getOrCreateSNSSQSSubsription(queueArn, topicArn string) (string, error) {
	var (
		subscriptionArn string
		err             error
		ok              bool
	)

	compositeKey := fmt.Sprintf("%s:%s", queueArn, topicArn)
	subscriptionArn, ok = s.subscriptions[compositeKey]
	if ok {
		s.logger.Debugf("Found subscription of queue arn: %s to topic arn: %s: %s", queueArn, topicArn, subscriptionArn)

		return subscriptionArn, nil
	}

	s.logger.Debugf("No subscription arn found of queue arn:%s to topic arn: %s\nCreating subscription", queueArn, topicArn)
	if !s.metadata.disableEntityManagement {
		subscriptionArn, err = s.createSNSSQSSubscription(queueArn, topicArn)
		if err != nil {
			s.logger.Errorf("Error creating subscription %s: %v", subscriptionArn, err)

			return "", err
		}
	} else {
		subscriptionArn, err = s.getSNSSQSSubscriptionArn(topicArn)
		if err != nil {
			s.logger.Errorf("error fetching info for topic arn %s: %w", topicArn, err)

			return "", err
		}
	}

	s.subscriptions[compositeKey] = subscriptionArn
	s.logger.Debugf("Subscribed to topic %s: %s", topicArn, subscriptionArn)

	return subscriptionArn, nil
}

func (s *snsSqs) Publish(req *pubsub.PublishRequest) error {
	topicArn, err := s.getOrCreateTopic(req.Topic)
	if err != nil {
		s.logger.Errorf("error getting topic ARN for %s: %v", req.Topic, err)
	}

	message := string(req.Data)
	snsPublishInput := &sns.PublishInput{
		Message:  &message,
		TopicArn: &topicArn,
	}
	if s.metadata.fifo {
		snsPublishInput.MessageGroupId = s.getMessageGroupID(req)
	}

	_, err = s.snsClient.Publish(snsPublishInput)
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
	// already been moved to the dead-letters queue by SQS. meaning, the below condition should not be reached as SQS would not send
	// a message if we've already surpassed the s.metadata.messageReceiveLimit value.
	if deadLettersQueueInfo != nil && recvCountInt > s.metadata.messageReceiveLimit {
		awsErr := fmt.Errorf(
			"message received greater than %v times, this message should have been moved without further processing to dead-letters queue: %v", s.metadata.messageReceiveLimit, s.metadata.sqsDeadLettersQueueName)
		s.logger.Error(awsErr)
		return awsErr
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
		wrappedErr := fmt.Errorf("error retrieving SQS dead-letter queue: %w", err)
		s.logger.Error(wrappedErr)

		return nil, wrappedErr
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
		Principal: `{"Service": "sns.amazonaws.com"}`,
		Action:    "sqs:SendMessage",
		Resource:  sqsQueueInfo.arn,
		Condition: condition{
			ArnEquals: arnEquals{
				AwsSourceArn: snsARN,
			},
		},
	}

	policy := &policy{Version: "2012-11-05"}
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
		wrappedErr := fmt.Errorf("error getting topic ARN for %s: %w", req.Topic, err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	// this is the ID of the application, it is supplied via runtime as "consumerID".
	var queueInfo *sqsQueueInfo
	queueInfo, err = s.getOrCreateQueue(s.metadata.sqsQueueName)
	if err != nil {
		wrappedErr := fmt.Errorf("error retrieving SQS queue: %w", err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	// only after a SQS queue and SNS topic had been setup, we restrict the SendMessage action to SNS as sole source
	// to prevent anyone but SNS to publish message to SQS.
	err = s.restrictQueuePublishPolicyToOnlySNS(queueInfo, topicArn)
	if err != nil {
		wrappedErr := fmt.Errorf("error setting sns-sqs subscription policy: %w", err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	// apply the dead letters queue attributes to the current queue.
	var deadLettersQueueInfo *sqsQueueInfo
	if len(s.metadata.sqsDeadLettersQueueName) > 0 {
		var derr error
		deadLettersQueueInfo, derr = s.createDeadLettersQueue()
		if derr != nil {
			wrappedErr := fmt.Errorf("error creating dead-letter queue: %w", derr)
			s.logger.Error(wrappedErr)

			return wrappedErr
		}

		var sqsSetQueueAttributesInput *sqs.SetQueueAttributesInput
		sqsSetQueueAttributesInput, derr = s.createQueueAttributesWithDeadLetters(queueInfo, deadLettersQueueInfo)
		if derr != nil {
			wrappedErr := fmt.Errorf("error creatubg queue attributes for dead-letter queue: %w", derr)
			s.logger.Error(wrappedErr)

			return wrappedErr
		}

		_, derr = s.sqsClient.SetQueueAttributes(sqsSetQueueAttributesInput)
		if derr != nil {
			wrappedErr := fmt.Errorf("error updating queue attributes with dead-letter queue: %w", derr)
			s.logger.Error(wrappedErr)

			return wrappedErr
		}
	}

	// subscription creation is idempotent. Subscriptions are unique by topic/queue.
	if _, err := s.getOrCreateSNSSQSSubsription(queueInfo.arn, topicArn); err != nil {
		wrappedErr := fmt.Errorf("error subscribing topic: %s, to queue: %s, with error: %w", topicArn, queueInfo.arn, err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	s.consumeSubscription(queueInfo, deadLettersQueueInfo, handler)

	return nil
}

func (s *snsSqs) Close() error {
	return nil
}

func (s *snsSqs) Features() []pubsub.Feature {
	return nil
}
