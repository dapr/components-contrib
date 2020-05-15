package snssqs

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/dapr/dapr/pkg/logger"
	"strconv"
	"strings"

	//aws_client "github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	sns "github.com/aws/aws-sdk-go/service/sns"
	sqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/dapr/components-contrib/pubsub"
)

type snsSqs struct {
	// Key is the topic name, value is the ARN of the topic
	topics map[string]string
	// Key is the topic name, value holds the ARN of the queue and its url
	queues    map[string]*sqsQueueInfo
	awsAcctId string
	snsClient *sns.SNS
	sqsClient *sqs.SQS
	metadata  *snsSqsMetadata
	logger    logger.Logger
}

type sqsQueueInfo struct {
	arn string
	url string
}

type snsSqsMetadata struct {
	// The name of the queue for this application. The is provided by the runtime as "consumerID"
	sqsQueueName string

	// The AWS endpoint for the component to use.
	awsEndpoint string
	// The AWS account ID to use for SNS/SQS. Required
	awsAccountId string
	// The AWS secret corresponding to the account ID. Required
	awsSecret string
	// The AWS token to use. Required
	awsToken string
	// The AWS region in which SNS/SQS should create resources. Required
	awsRegion string

	// Amount of time in seconds that a message is hidden from receive requests after it is sent to a subscriber. Default: 10
	messageVisibilityTimeout int64
	// Number of times to resend a message after processing of that message fails before removing that message from the queue. Default: 10
	messageRetryLimit int64
	// Amount of time to await receipt of a message before making another request. Default: 1
	messageWaitTimeSeconds int64
	// Maximum number of messsages to receive from the queue at a time. Default: 10, Maximum: 10
	messageMaxNumber int64
}

func NewSnsSqs(l logger.Logger) pubsub.PubSub {
	return &snsSqs{logger: l}
}

func parseInt64 (input string, propertyName string) (int64, error) {
	number, err := strconv.Atoi(input)

	if err != nil {
		return -1, errors.New(fmt.Sprintf("Parsing %s failed with: %v", propertyName, err))
	}
	return int64(number), nil
}

// Take a name and hash it for compatibility with AWS resource names
// The output is fixed at 64 characters
// TODO plumb this is, affix the plaintext name as a tag attribute
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

	if val, ok := props["awsEndpoint"]; ok {
		md.awsEndpoint = val
	}

	val, ok := props["awsAccountId"]

	if !ok {
		return nil, errors.New("Missing required property: awsAccountId")
	}

	md.awsAccountId = val

	val, ok = props["awsSecret"]
	if !ok {
		return nil, errors.New("Missing required property: awsSecret")
	}

	md.awsSecret = val

	val, ok = props["awsToken"]
	if !ok {
		return nil, errors.New("Missing required property: awsToken")
	}

	md.awsToken = val

	val, ok = props["awsRegion"]
	if !ok {
		return nil, errors.New("Missing required property: awsRegion")
	}

	md.awsRegion = val

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
	// Either publish or subscribe needs reference to a TopicARN
	// So we should keep a map of topic ARNs
	// This map should be written to whenever
	md, err := s.getSnsSqsMetatdata(metadata)

	if err != nil {
		return err
	}

	s.metadata = md

	s.topics = make(map[string]string)
	s.queues = make(map[string]*sqsQueueInfo)
	config := aws.NewConfig()
	endpoint := md.awsEndpoint 
	s.awsAcctId = md.awsAccountId
	config.Credentials = credentials.NewStaticCredentials(s.awsAcctId, md.awsSecret, md.awsToken)
	config.Endpoint = &endpoint
	config.Region = aws.String(md.awsRegion)
	sesh := session.Must(session.NewSession(config))
	s.snsClient = sns.New(sesh)
	s.sqsClient = sqs.New(sesh)

	return nil
}
// FIXME: Topic name must not contain slashes, the topic name will have to be stored in some other place, perhaps as a tag
// or the topic name could be encoded as a SHA-256
// Create the topic, return the topic's ARN
func (s *snsSqs) createTopic(topic string) (string, error) {
	createTopicResponse, err := s.snsClient.CreateTopic(&sns.CreateTopicInput{
		//Attributes: nil,
		Name: aws.String(topic),
		//Tags:       nil, TODO add something to indicate that this topic was created by dapr
	})

	if err != nil {
		return "", err
	}

	return *(createTopicResponse.TopicArn), nil
}

// Get the topic ARN from the topics map. If it doesn't exist in the map, try to fetch it from AWS, if it doesn't exist
// at all, issue a request to create the topic.
func (s *snsSqs) getOrCreateTopic(topic string) (string, error) {
	topicArn, ok := s.topics[topic]

	if ok {
		s.logger.Debugf("Found existing topic ARN for topic %s: %s", topic, topicArn)
		return topicArn, nil
	}

	s.logger.Debugf("No topic ARN found for %s\n Creating topic instead.", topic)

	topicArn, err := s.createTopic(topic)

	if err != nil {
		s.logger.Errorf("Error creating new topic %s: %v", topic, err)
		return "", err
	}

	// Record topic ARN
	s.topics[topic] = topicArn

	return topicArn, nil
}

// FIXME: Queue name is not guaranteed to contain safe character for AWS queuenames, probably encode as SHA-256
func (s *snsSqs) createQueue(queueName string) (*sqsQueueInfo, error) {
	createQueueResponse, err := s.sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		//Tags:       nil, TODO add something indicating that this was created with dapr
	})

	if err != nil {
		return nil, err
	}

	queueAttributesResponse, err := s.sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("QueueArn")},
		QueueUrl:       createQueueResponse.QueueUrl,
	})

	if err != nil {
		s.logger.Errorf("Error fetching queue attributes for %s: %v", queueName, err)
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
		s.logger.Errorf("Error getting topic ARN for %s: %v", req.Topic, err)
	}

	message := string(req.Data)
	publishOutput, err := s.snsClient.Publish(&sns.PublishInput{
		Message:  &message,
		TopicArn: &topicArn,
	})

	if err != nil {
		s.logger.Errorf("Error publishing topic %s with topic ARN %s: %v", req.Topic, topicArn, err)
		return err
	}

	s.logger.Debugf("Message published: %v\n%v", message, publishOutput)
	return nil
}

type snsMessage struct {
	Message  string
	TopicArn string
}

func parseTopicArn(arn string) string {
	return arn[strings.LastIndex(arn, ":")+1:]
}

func (s *snsSqs) acknowledgeMessage(queueUrl string, receiptHandle *string) error {
	_, err := s.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &queueUrl,
		ReceiptHandle: receiptHandle,
	})

	return err
}

func (s *snsSqs) handleMessage(message *sqs.Message, queueInfo *sqsQueueInfo, handler func(msg *pubsub.NewMessage) error) error {
	// If this message has been received > x times, delete from queue, it's borked
	recvCount, ok := message.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]

	if !ok {
		return errors.New(
			fmt.Sprintf(
				"No ApproximateReceiveCount returned with response, will not attempt further processing: %v", message))

	}

	recvCountInt, err := strconv.ParseInt(*recvCount, 10, 32)

	if err != nil {
		return errors.New(fmt.Sprintf("Error parsing ApproximateReceiveCount from message: %v", message))
	}

	// If we are over the allowable retry limit, delete the message from the queue
	// TODO dead letter queue
	if recvCountInt >= s.metadata.messageRetryLimit {
		if err := s.acknowledgeMessage(queueInfo.url, message.ReceiptHandle); err != nil {
			return errors.New(
				fmt.Sprintf(
					"Error acknowledging message after receiving the message too many times: %v", err))
		}

		return errors.New(
			fmt.Sprintf(
				"Message received greater than %v times, deleting this message without further processing", s.metadata.messageRetryLimit))
	}

	// Otherwise try to handle the message

	var messageBody snsMessage
	err = json.Unmarshal([]byte(*(message.Body)), &messageBody)

	if err != nil {
		return errors.New(fmt.Sprintf("Error unmarshalling message: %v", err))
	}

	topic := parseTopicArn(messageBody.TopicArn)
	err = handler(&pubsub.NewMessage{
		Data:  []byte(messageBody.Message),
		Topic: topic,
	})

	if err != nil {
		return errors.New(fmt.Sprintf("Error handling message: %v", err))
	}

	// Otherwise, there was no error, acknowledge the message
	return s.acknowledgeMessage(queueInfo.url, message.ReceiptHandle)
}

func (s *snsSqs) consumeSubscription(queueInfo *sqsQueueInfo, handler func(msg *pubsub.NewMessage) error) {
	go func() {
		for {
			messageResponse, err := s.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
				// Use this property to decide when a message should be discarded
				AttributeNames: []*string{
					aws.String(sqs.MessageSystemAttributeNameApproximateReceiveCount),
				},
				MaxNumberOfMessages: aws.Int64(s.metadata.messageMaxNumber),
				QueueUrl:            &queueInfo.url,
				VisibilityTimeout:   aws.Int64(s.metadata.messageVisibilityTimeout),
				WaitTimeSeconds:     aws.Int64(s.metadata.messageWaitTimeSeconds),
			})

			if err != nil {
				s.logger.Errorf("Error consuming topic: %v", err)
				continue
			}

			// Retry receiving messages
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
	// Subscribers declare a topic ARN
	// and declare a SQS queue to use
	// These should be idempotent
	// Queues should not be created if they exist
	topicArn, err := s.getOrCreateTopic(req.Topic)

	if err != nil {
		s.logger.Errorf("Error getting topic ARN for %s: %v", req.Topic, err)
		return err
	}

	// This is the ID of the application, it is supplied via runtime as "consumerID"
	queueInfo, err := s.getOrCreateQueue(s.metadata.sqsQueueName)

	if err != nil {
		s.logger.Errorf("Error retrieving SQS queue: %v", err)
		return err
	}

	// Subscription creation is idempotent. Subscriptions are unique by topic/queue
	subscribeOutput, err := s.snsClient.Subscribe(&sns.SubscribeInput{
		Attributes:            nil,
		Endpoint:              &queueInfo.arn, // create SQS queue per subscription
		Protocol:              aws.String("sqs"),
		ReturnSubscriptionArn: nil,
		TopicArn:              &topicArn,
	})

	if err != nil {
		s.logger.Errorf("Error subscribing to topic %s: %v", req.Topic, err)
		return err
	}

	s.logger.Debugf("Subscribed to topic %s: %v", req.Topic, subscribeOutput)

	s.consumeSubscription(queueInfo, handler)

	return nil
}
