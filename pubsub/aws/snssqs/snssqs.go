package snssqs

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/dapr/dapr/pkg/logger"
	"strings"

	//aws_client "github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	sns "github.com/aws/aws-sdk-go/service/sns"
	sqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/dapr/components-contrib/pubsub"
)

type snsSqs struct {
	topics    map[string]string        // Key is the topic name, value is the ARN of the topic
	queues    map[string]*sqsQueueInfo // Key is the topic name, value is the ARN of the queue
	awsAcctId string
	snsClient *sns.SNS
	sqsClient *sqs.SQS
	logger    logger.Logger
}

type sqsQueueInfo struct {
	arn string
	url string
}

type snsSqsMetadata struct {
}

func NewSnsSqs(l logger.Logger) pubsub.PubSub {
	return &snsSqs{logger: l}
}

func (s *snsSqs) Init(metadata pubsub.Metadata) error {
	// Either publish or subscribe needs reference to a TopicARN
	// So we should keep a map of topic ARNs
	// This map should be written to whenever
	s.topics = make(map[string]string)
	s.queues = make(map[string]*sqsQueueInfo)
	config := aws.NewConfig()
	endpoint := "http://localhost:4566" // TODO don't hardcode endpoint
	s.awsAcctId = "fizzle" // TODO get from metadata
	config.Credentials = credentials.NewStaticCredentials(s.awsAcctId, "fazzle", "wazzle") // TODO metadata
	config.Endpoint = &endpoint
	config.Region = strPtr(endpoints.UsEast1RegionID) // TODO metadata
	sesh := session.Must(session.NewSession(config))
	s.snsClient = sns.New(sesh)
	s.sqsClient = sqs.New(sesh)

	return nil
}

// Create the topic, return the topic's ARN
func (s *snsSqs) createTopic(topic string) (string, error) {
	createTopicResponse, err := s.snsClient.CreateTopic(&sns.CreateTopicInput{
		//Attributes: nil,
		Name: strPtr(topic),
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

func strPtr(stringer string) *string {
	return &stringer
}

func (s *snsSqs) createQueue(queueName string) (*sqsQueueInfo, error) {
	createQueueResponse, err := s.sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: strPtr(queueName),
		//Tags:       nil, TODO add something indicating that this was created with dapr
	})

	if err != nil {
		return nil, err
	}

	queueAttributesResponse, err := s.sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{strPtr("QueueArn")},
		QueueUrl:       createQueueResponse.QueueUrl,
	})

	if err != nil {
		s.logger.Errorf("Error fetching queue attributes for %s: %v", queueName, err)
	}

	//info :=

	//bing, bong := s.sqsClient.SetQueueAttributes(&sqs.SetQueueAttributesInput{
	//	// TODO Doesn't seem to be necessary
	//	//Attributes: map[string]*string{
	//	//	"Policy": aws.String(fmt.Sprintf(`{
	//	//			"Statement": [{
	//	//				"Effect":"Allow",
	//	//				"Principal":"*",
	//	//				"Action":"sqs:SendMessage",
	//	//				"Resource":"%s",
	//	//				"Condition":{
	//	//					"ArnEquals":{
	//	//						"aws:SourceArn":"%s""
	//	//					}
	//	//				}
	//	//			}]
	//	//		}`, info.arn, s.topics["swoosh"])),
	//	//},
	//	QueueUrl: aws.String(info.url),
	//})

	return &sqsQueueInfo{
		arn: *(queueAttributesResponse.Attributes["QueueArn"]),
		url: *(createQueueResponse.QueueUrl),
	}, nil
}

func (s *snsSqs) getOrCreateQueue(queueName string) (*sqsQueueInfo, error) {
	// TODO implement a get or create for sqs queues
	queueArn, ok := s.queues[queueName]

	if ok {
		s.logger.Debugf("Found queue arn for %s: %s", queueName, queueArn)
		return queueArn, nil
	}
	// TODO here, creating queues doesn't seem to be idempotent, but the name should be unique
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
		Message: &message,
		//MessageAttributes: nil,
		//MessageStructure:  strPtr("text"), // "json",
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

func (s *snsSqs) consumeSubscription(queueInfo *sqsQueueInfo, handler func(msg *pubsub.NewMessage) error) {
	go func() {
		for {
			messageResponse, err := s.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
				AttributeNames: []*string{ // TODO figure out why tf adding these bingers made is so that I can receive a message
					aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
				},
				MessageAttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				MaxNumberOfMessages:     aws.Int64(1),
				QueueUrl:                &queueInfo.url,
				ReceiveRequestAttemptId: nil,
				VisibilityTimeout:       aws.Int64(10),
				WaitTimeSeconds:         aws.Int64(2),
			})

			if err != nil {
				s.logger.Errorf("Error consuming topic") // TODO plumb in topic name, other information
			}

			// Retry receiving messages
			if len(messageResponse.Messages) < 1 {
				s.logger.Debug("No messages received, taking it around the block again")
				continue
			}
			//changing the fucking  typ eo fthe message from json to text made it work, need to verify if removing the permission policies works
			s.logger.Debug("Message received!")

			for _, m := range messageResponse.Messages {
				// TODO Retry failures here? Fuck I hope not
				var messageBody snsMessage
				err = json.Unmarshal([]byte(*(m.Body)), &messageBody)

				// TODO handle error from unmarshal
				topic := parseTopicArn(messageBody.TopicArn)
				err = handler(&pubsub.NewMessage{
					Data:  []byte(messageBody.Message),
					Topic: topic, //TODO AWS hates pubsub how tf do you get a topic name from a fucking message in a queue
				})

				deleteMessageResponse, err := s.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      &queueInfo.url,
					ReceiptHandle: m.ReceiptHandle,
				})

				if err != nil {
					// Retry acknowledgement?
					s.logger.Errorf("Error acknowledging message: %v\n%v", err, deleteMessageResponse)
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

	// TODO plumb in consumer ID
	queueInfo, err := s.getOrCreateQueue("qqnoob") // This should be the application ID

	if err != nil {
		s.logger.Errorf("Error retrieving SQS queue: %v", err)
		return err
	}

	// TODO, check if subscription already exists, something ListSubscriptions ...ByTopic
	subscribeOutput, err := s.snsClient.Subscribe(&sns.SubscribeInput{
		Attributes:            nil,
		Endpoint:              &queueInfo.arn, // SQS ARN TODONE! create SQS queue per subscription
		Protocol:              strPtr("sqs"),
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
