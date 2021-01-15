package snssqs

import (
	"crypto/sha256"
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
	"github.com/dapr/dapr/pkg/logger"
)

type snsSqs struct {
	// key is the topic name, value is the ARN of the topic
	topics map[string]string
	// key is the hashed topic name, value is the actual topic name
	topicHash map[string]string
	// key is the topic name, value holds the ARN of the queue and its url
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
	// name of the queue for this application. The is provided by the runtime as "consumerID"
	sqsQueueName string

	// aws endpoint for the component to use.
	Endpoint string
	// access key to use for accessing sqs/sns
	AccessKey string
	// secret key to use for accessing sqs/sns
	SecretKey string
	// aws session token to use.
	SessionToken string
	// aws region in which SNS/SQS should create resources
	Region string

	// amount of time in seconds that a message is hidden from receive requests after it is sent to a subscriber. Default: 10
	messageVisibilityTimeout int64
	// number of times to resend a message after processing of that message fails before removing that message from the queue. Default: 10
	messageRetryLimit int64
	// amount of time to await receipt of a message before making another request. Default: 1
	messageWaitTimeSeconds int64
	// maximum number of messages to receive from the queue at a time. Default: 10, Maximum: 10
	messageMaxNumber int64
}

const (
	awsSqsQueueNameKey = "dapr-queue-name"
	awsSnsTopicNameKey = "dapr-topic-name"
)

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
		return -1, fmt.Errorf("parsing %s failed with: %v", propertyName, err)
	}

	return int64(number), nil
}

// take a name and hash it for compatibility with AWS resource names
// the output is fixed at 64 characters
func nameToHash(name string) string {
	h := sha256.New()
	h.Write([]byte(name))

	return fmt.Sprintf("%x", h.Sum(nil))
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
	// track these ARNs in this map
	s.topics = make(map[string]string)
	s.topicHash = make(map[string]string)
	s.queues = make(map[string]*sqsQueueInfo)
	sess, err := aws_auth.GetClient(md.AccessKey, md.SecretKey, md.SessionToken, md.Region, md.Endpoint)
	if err != nil {
		return err
	}
	s.snsClient = sns.New(sess)
	s.sqsClient = sqs.New(sess)

	return nil
}

func (s *snsSqs) createTopic(topic string) (string, string, error) {
	hashedName := nameToHash(topic)
	createTopicResponse, err := s.snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(hashedName),
		Tags: []*sns.Tag{{Key: aws.String(awsSnsTopicNameKey), Value: aws.String(topic)}},
	})
	if err != nil {
		return "", "", err
	}

	return *(createTopicResponse.TopicArn), hashedName, nil
}

// get the topic ARN from the topics map. If it doesn't exist in the map, try to fetch it from AWS, if it doesn't exist
// at all, issue a request to create the topic.
func (s *snsSqs) getOrCreateTopic(topic string) (string, error) {
	topicArn, ok := s.topics[topic]

	if ok {
		s.logger.Debugf("Found existing topic ARN for topic %s: %s", topic, topicArn)

		return topicArn, nil
	}

	s.logger.Debugf("No topic ARN found for %s\n Creating topic instead.", topic)

	topicArn, hashedName, err := s.createTopic(topic)
	if err != nil {
		s.logger.Errorf("error creating new topic %s: %v", topic, err)

		return "", err
	}

	// record topic ARN
	s.topics[topic] = topicArn
	s.topicHash[hashedName] = topic

	return topicArn, nil
}

func (s *snsSqs) createQueue(queueName string) (*sqsQueueInfo, error) {
	createQueueResponse, err := s.sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(nameToHash(queueName)),
		Tags:      map[string]*string{awsSqsQueueNameKey: aws.String(queueName)},
	})
	if err != nil {
		return nil, err
	}

	queueAttributesResponse, err := s.sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("QueueArn")},
		QueueUrl:       createQueueResponse.QueueUrl,
	})
	if err != nil {
		s.logger.Errorf("error fetching queue attributes for %s: %v", queueName, err)
	}

	// add permissions to allow SNS to send messages to this queue
	_, err = s.sqsClient.SetQueueAttributes(&(sqs.SetQueueAttributesInput{
		Attributes: map[string]*string{
			"Policy": aws.String(fmt.Sprintf(`{
				"Statement": [{
					"Effect":"Allow",
					"Principal":"*",
					"Action":"sqs:SendMessage",
					"Resource":"%s"				
				}]
			}`, *(queueAttributesResponse.Attributes["QueueArn"]))),
		},
		QueueUrl: createQueueResponse.QueueUrl,
	}))

	if err != nil {
		return nil, err
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
	// creating queues is idempotent, the names serve as unique keys among a given region
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
		s.logger.Errorf("error publishing topic %s with topic ARN %s: %v", req.Topic, topicArn, err)

		return err
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
	_, err := s.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: receiptHandle,
	})

	return err
}

func (s *snsSqs) handleMessage(message *sqs.Message, queueInfo *sqsQueueInfo, handler func(msg *pubsub.NewMessage) error) error {
	// if this message has been received > x times, delete from queue, it's borked
	recvCount, ok := message.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]

	if !ok {
		return fmt.Errorf(
			"no ApproximateReceiveCount returned with response, will not attempt further processing: %v", message)
	}

	recvCountInt, err := strconv.ParseInt(*recvCount, 10, 32)
	if err != nil {
		return fmt.Errorf("error parsing ApproximateReceiveCount from message: %v", message)
	}

	// if we are over the allowable retry limit, delete the message from the queue
	// TODO dead letter queue
	if recvCountInt >= s.metadata.messageRetryLimit {
		if innerErr := s.acknowledgeMessage(queueInfo.url, message.ReceiptHandle); innerErr != nil {
			return fmt.Errorf("error acknowledging message after receiving the message too many times: %v", innerErr)
		}

		return fmt.Errorf(
			"message received greater than %v times, deleting this message without further processing", s.metadata.messageRetryLimit)
	}

	// otherwise try to handle the message
	var messageBody snsMessage
	err = json.Unmarshal([]byte(*(message.Body)), &messageBody)

	if err != nil {
		return fmt.Errorf("error unmarshalling message: %v", err)
	}

	topic := parseTopicArn(messageBody.TopicArn)
	topic = s.topicHash[topic]
	err = handler(&pubsub.NewMessage{
		Data:  []byte(messageBody.Message),
		Topic: topic,
	})

	if err != nil {
		return fmt.Errorf("error handling message: %v", err)
	}

	// otherwise, there was no error, acknowledge the message
	return s.acknowledgeMessage(queueInfo.url, message.ReceiptHandle)
}

func (s *snsSqs) consumeSubscription(queueInfo *sqsQueueInfo, handler func(msg *pubsub.NewMessage) error) {
	go func() {
		for {
			messageResponse, err := s.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
				// use this property to decide when a message should be discarded
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

			// retry receiving messages
			if len(messageResponse.Messages) < 1 {
				s.logger.Debug("No messages received, requesting again")

				continue
			}

			s.logger.Debugf("%v message(s) received", len(messageResponse.Messages))

			for _, m := range messageResponse.Messages {
				if err := s.handleMessage(m, queueInfo, handler); err != nil {
					s.logger.Error(err)
				}
			}
		}
	}()
}

func (s *snsSqs) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	// subscribers declare a topic ARN
	// and declare a SQS queue to use
	// these should be idempotent
	// queues should not be created if they exist
	topicArn, err := s.getOrCreateTopic(req.Topic)
	if err != nil {
		s.logger.Errorf("error getting topic ARN for %s: %v", req.Topic, err)

		return err
	}

	// this is the ID of the application, it is supplied via runtime as "consumerID"
	queueInfo, err := s.getOrCreateQueue(s.metadata.sqsQueueName)
	if err != nil {
		s.logger.Errorf("error retrieving SQS queue: %v", err)

		return err
	}

	// subscription creation is idempotent. Subscriptions are unique by topic/queue
	subscribeOutput, err := s.snsClient.Subscribe(&sns.SubscribeInput{
		Attributes:            nil,
		Endpoint:              &queueInfo.arn, // create SQS queue per subscription
		Protocol:              aws.String("sqs"),
		ReturnSubscriptionArn: nil,
		TopicArn:              &topicArn,
	})
	if err != nil {
		s.logger.Errorf("error subscribing to topic %s: %v", req.Topic, err)

		return err
	}

	s.subscriptions = append(s.subscriptions, subscribeOutput.SubscriptionArn)
	s.logger.Debugf("Subscribed to topic %s: %v", req.Topic, subscribeOutput)

	s.consumeSubscription(queueInfo, handler)

	return nil
}

func (s *snsSqs) Close() error {
	for _, sub := range s.subscriptions {
		s.snsClient.Unsubscribe(&sns.UnsubscribeInput{
			SubscriptionArn: sub,
		})
	}

	return nil
}

func (s *snsSqs) Features() []pubsub.Feature {
	return nil
}
