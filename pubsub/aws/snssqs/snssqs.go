/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package snssqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"

	"github.com/dapr/kit/retry"

	gonanoid "github.com/matoous/go-nanoid/v2"

	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type snsSqs struct {
	topicLock sync.RWMutex
	// key is the sanitized topic name
	topicArns map[string]string
	// key is the topic name, value holds the ARN of the queue and its url.
	queues map[string]*sqsQueueInfo
	// key is a composite key of queue ARN and topic ARN mapping to subscription ARN.
	subscriptions       map[string]string
	snsClient           *sns.SNS
	sqsClient           *sqs.SQS
	stsClient           *sts.STS
	metadata            *snsSqsMetadata
	logger              logger.Logger
	id                  string
	opsTimeout          time.Duration
	backOffConfig       retry.Config
	subscriptionManager SubscriptionManagement
	closed              atomic.Bool
}

type sqsQueueInfo struct {
	arn string
	url string
}

type snsMessage struct {
	Message  string
	TopicArn string
}

func (sn *snsMessage) parseTopicArn() string {
	arn := sn.TopicArn
	return arn[strings.LastIndex(arn, ":")+1:]
}

const (
	awsSqsQueueNameKey                    = "dapr-queue-name"
	awsSnsTopicNameKey                    = "dapr-topic-name"
	awsSqsFifoSuffix                      = ".fifo"
	maxAWSNameLength                      = 80
	assetsManagementDefaultTimeoutSeconds = 5.0
	awsAccountIDLength                    = 12
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

func (s *snsSqs) Init(ctx context.Context, metadata pubsub.Metadata) error {
	md, err := s.getSnsSqsMetatdata(metadata)
	if err != nil {
		return err
	}

	s.metadata = md

	sess, err := awsAuth.GetClient(md.AccessKey, md.SecretKey, md.SessionToken, md.Region, md.Endpoint)
	if err != nil {
		return fmt.Errorf("error creating an AWS client: %w", err)
	}
	// AWS sns,sqs,sts client.
	s.snsClient = sns.New(sess)
	s.sqsClient = sqs.New(sess)
	s.stsClient = sts.New(sess)

	s.opsTimeout = time.Duration(md.AssetsManagementTimeoutSeconds * float64(time.Second))

	err = s.setAwsAccountIDIfNotProvided(ctx)
	if err != nil {
		return err
	}

	// Default retry configuration is used if no
	// backOff properties are set.
	err = retry.DecodeConfigWithPrefix(&s.backOffConfig, metadata.Properties, "backOff")
	if err != nil {
		return fmt.Errorf("error decoding backOff config: %w", err)
	}
	// subscription manager responsible for managing the lifecycle of subscriptions.
	s.subscriptionManager = NewSubscriptionMgmt(s.logger)

	s.queues = make(map[string]*sqsQueueInfo)
	s.subscriptions = make(map[string]string)
	s.topicArns = make(map[string]string)

	return nil
}

func (s *snsSqs) setAwsAccountIDIfNotProvided(parentCtx context.Context) error {
	if len(s.metadata.AccountID) == awsAccountIDLength {
		return nil
	}

	ctx, cancelFn := context.WithTimeout(parentCtx, s.opsTimeout)
	callerIDOutput, err := s.stsClient.GetCallerIdentityWithContext(ctx, &sts.GetCallerIdentityInput{})
	cancelFn()
	if err != nil {
		return fmt.Errorf("error fetching sts caller ID: %w", err)
	}

	s.metadata.AccountID = *callerIDOutput.Account
	return nil
}

func (s *snsSqs) buildARN(serviceName, entityName string) string {
	return fmt.Sprintf("arn:%s:%s:%s:%s:%s", s.metadata.internalPartition, serviceName, s.metadata.Region, s.metadata.AccountID, entityName)
}

func (s *snsSqs) createTopic(parentCtx context.Context, topic string) (string, error) {
	sanitizedName := nameToAWSSanitizedName(topic, s.metadata.Fifo)
	snsCreateTopicInput := &sns.CreateTopicInput{
		Name: aws.String(sanitizedName),
		Tags: []*sns.Tag{{Key: aws.String(awsSnsTopicNameKey), Value: aws.String(topic)}},
	}

	if s.metadata.Fifo {
		attributes := map[string]*string{"FifoTopic": aws.String("true"), "ContentBasedDeduplication": aws.String("true")}
		snsCreateTopicInput.SetAttributes(attributes)
	}

	ctx, cancelFn := context.WithTimeout(parentCtx, s.opsTimeout)
	createTopicResponse, err := s.snsClient.CreateTopicWithContext(ctx, snsCreateTopicInput)
	cancelFn()
	if err != nil {
		return "", fmt.Errorf("error while creating an SNS topic: %w", err)
	}

	return *(createTopicResponse.TopicArn), nil
}

func (s *snsSqs) getTopicArn(parentCtx context.Context, topic string) (string, error) {
	ctx, cancelFn := context.WithTimeout(parentCtx, s.opsTimeout)
	arn := s.buildARN("sns", topic)
	getTopicOutput, err := s.snsClient.GetTopicAttributesWithContext(ctx, &sns.GetTopicAttributesInput{
		TopicArn: &arn,
	})
	cancelFn()
	if err != nil {
		return "", fmt.Errorf("error: %w, while getting (sanitized) topic: %v with arn: %v", err, topic, arn)
	}

	return *getTopicOutput.Attributes["TopicArn"], nil
}

// Get the topic ARN from the topics map. If it doesn't exist in the map, try to fetch it from AWS, if it doesn't exist
// at all, issue a request to create the topic.
// NOTE: This method potentially reads and writes to the topicArns map, which may end up being accessed by multiple goroutines concurrently,
// therefore it is necessary for its caller to lock the topic.
func (s *snsSqs) getOrCreateTopic(ctx context.Context, topic string) (topicArn string, sanitizedTopic string, err error) {
	sanitizedTopic = nameToAWSSanitizedName(topic, s.metadata.Fifo)

	var exists bool
	s.topicLock.RLock()
	topicArn, exists = s.topicArns[sanitizedTopic]
	s.topicLock.RUnlock()

	if exists {
		s.logger.Debugf("Found existing topic ARN for topic %s: %s", topic, topicArn)
		return topicArn, sanitizedTopic, err
	}

	// creating queues is idempotent, the names serve as unique keys among a given region.
	s.logger.Debugf("No SNS topic ARN found for topic: %s. creating SNS with (sanitized) topic: %s", topic, sanitizedTopic)

	if !s.metadata.DisableEntityManagement {
		topicArn, err = s.createTopic(ctx, sanitizedTopic)
		if err != nil {
			err = fmt.Errorf("error creating new (sanitized) topic '%s': %w", topic, err)

			return topicArn, sanitizedTopic, err
		}
	} else {
		topicArn, err = s.getTopicArn(ctx, sanitizedTopic)
		if err != nil {
			err = fmt.Errorf("error fetching info for (sanitized) topic: %s. wrapped error is: %w", topic, err)

			return topicArn, sanitizedTopic, err
		}
	}

	// record topic ARN.
	s.topicLock.Lock()
	s.topicArns[sanitizedTopic] = topicArn
	s.topicLock.Unlock()

	return topicArn, sanitizedTopic, err
}

func (s *snsSqs) createQueue(parentCtx context.Context, queueName string) (*sqsQueueInfo, error) {
	sanitizedName := nameToAWSSanitizedName(queueName, s.metadata.Fifo)
	sqsCreateQueueInput := &sqs.CreateQueueInput{
		QueueName: aws.String(sanitizedName),
		Tags:      map[string]*string{awsSqsQueueNameKey: aws.String(queueName)},
	}

	if s.metadata.Fifo {
		attributes := map[string]*string{"FifoQueue": aws.String("true"), "ContentBasedDeduplication": aws.String("true")}
		sqsCreateQueueInput.SetAttributes(attributes)
	}
	ctx, cancel := context.WithTimeout(parentCtx, s.opsTimeout)
	createQueueResponse, err := s.sqsClient.CreateQueueWithContext(ctx, sqsCreateQueueInput)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("error creaing an SQS queue: %w", err)
	}

	ctx, cancel = context.WithTimeout(parentCtx, s.opsTimeout)
	queueAttributesResponse, err := s.sqsClient.GetQueueAttributesWithContext(ctx, &sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("QueueArn")},
		QueueUrl:       createQueueResponse.QueueUrl,
	})
	cancel()
	if err != nil {
		s.logger.Errorf("error fetching queue attributes for %s: %v", queueName, err)
	}

	return &sqsQueueInfo{
		arn: *(queueAttributesResponse.Attributes["QueueArn"]),
		url: *(createQueueResponse.QueueUrl),
	}, nil
}

func (s *snsSqs) getQueueArn(parentCtx context.Context, queueName string) (*sqsQueueInfo, error) {
	ctx, cancel := context.WithTimeout(parentCtx, s.opsTimeout)
	queueURLOutput, err := s.sqsClient.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(queueName), QueueOwnerAWSAccountId: aws.String(s.metadata.AccountID)})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("error: %w while getting url of queue: %s", err, queueName)
	}
	url := queueURLOutput.QueueUrl

	ctx, cancel = context.WithTimeout(parentCtx, s.opsTimeout)
	getQueueOutput, err := s.sqsClient.GetQueueAttributesWithContext(ctx, &sqs.GetQueueAttributesInput{QueueUrl: url, AttributeNames: []*string{aws.String("QueueArn")}})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("error: %w while getting information for queue: %s, with url: %s", err, queueName, *url)
	}

	return &sqsQueueInfo{arn: *getQueueOutput.Attributes["QueueArn"], url: *url}, nil
}

func (s *snsSqs) getOrCreateQueue(ctx context.Context, queueName string) (*sqsQueueInfo, error) {
	var (
		err       error
		queueInfo *sqsQueueInfo
	)

	if cachedQueueInfo, ok := s.queues[queueName]; ok {
		s.logger.Debugf("Found queue ARN for %s: %s", queueName, cachedQueueInfo.arn)

		return cachedQueueInfo, nil
	}
	// creating queues is idempotent, the names serve as unique keys among a given region.
	s.logger.Debugf("No SQS queue ARN found for %s\nCreating SQS queue", queueName)

	sanitizedName := nameToAWSSanitizedName(queueName, s.metadata.Fifo)

	if !s.metadata.DisableEntityManagement {
		queueInfo, err = s.createQueue(ctx, sanitizedName)
		if err != nil {
			s.logger.Errorf("Error creating queue %s: %v", queueName, err)

			return nil, err
		}
	} else {
		queueInfo, err = s.getQueueArn(ctx, sanitizedName)
		if err != nil {
			s.logger.Errorf("error fetching info for queue %s: %w", queueName, err)

			return nil, err
		}
	}

	s.queues[queueName] = queueInfo
	s.logger.Debugf("created SQS queue: %s: with arn: %s", queueName, queueInfo.arn)

	return queueInfo, nil
}

func (s *snsSqs) getMessageGroupID(req *pubsub.PublishRequest) *string {
	if len(s.metadata.FifoMessageGroupID) > 0 {
		return &s.metadata.FifoMessageGroupID
	}
	// each daprd, of a given PubSub, of a given publisher application publishes to a message group ID of its own.
	// for example: for a daprd serving the SNS/SQS Pubsub component we generate a unique id -> A; that component serves on behalf
	// of a given PubSub deployment name B, and component A publishes to SNS on behalf of a dapr application named C (effectively to topic C).
	// therefore the created message group ID for publishing messages in the aforementioned setup is "A:B:C".
	fifoMessageGroupID := fmt.Sprintf("%s:%s:%s", s.id, req.PubsubName, req.Topic)
	return &fifoMessageGroupID
}

func (s *snsSqs) createSnsSqsSubscription(parentCtx context.Context, queueArn, topicArn string) (string, error) {
	ctx, cancel := context.WithTimeout(parentCtx, s.opsTimeout)
	subscribeOutput, err := s.snsClient.SubscribeWithContext(ctx, &sns.SubscribeInput{
		Attributes:            nil,
		Endpoint:              aws.String(queueArn), // create SQS queue per subscription.
		Protocol:              aws.String("sqs"),
		ReturnSubscriptionArn: nil,
		TopicArn:              aws.String(topicArn),
	})
	cancel()
	if err != nil {
		wrappedErr := fmt.Errorf("error subscribing to sns topic arn: %s, to queue arn: %s %w", topicArn, queueArn, err)
		s.logger.Error(wrappedErr)

		return "", wrappedErr
	}

	return *subscribeOutput.SubscriptionArn, nil
}

func (s *snsSqs) getSnsSqsSubscriptionArn(parentCtx context.Context, topicArn string) (string, error) {
	ctx, cancel := context.WithTimeout(parentCtx, s.opsTimeout)
	listSubscriptionsOutput, err := s.snsClient.ListSubscriptionsByTopicWithContext(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(topicArn)})
	cancel()
	if err != nil {
		return "", fmt.Errorf("error listing subsriptions for topic arn: %v: %w", topicArn, err)
	}

	for _, subscription := range listSubscriptionsOutput.Subscriptions {
		if *subscription.TopicArn == topicArn {
			return *subscription.SubscriptionArn, nil
		}
	}

	return "", errors.New("sns sqs subscription not found for topic arn")
}

func (s *snsSqs) getOrCreateSnsSqsSubscription(ctx context.Context, queueArn, topicArn string) (subscriptionArn string, err error) {
	compositeKey := fmt.Sprintf("%s:%s", queueArn, topicArn)
	if cachedSubscriptionArn, ok := s.subscriptions[compositeKey]; ok {
		s.logger.Debugf("Found subscription of queue arn: %s to topic arn: %s: %s", queueArn, topicArn, cachedSubscriptionArn)

		return cachedSubscriptionArn, nil
	}

	s.logger.Debugf("No subscription ARN found of queue arn:%s to topic arn: %s\nCreating subscription", queueArn, topicArn)

	if !s.metadata.DisableEntityManagement {
		subscriptionArn, err = s.createSnsSqsSubscription(ctx, queueArn, topicArn)
		if err != nil {
			s.logger.Errorf("Error creating subscription %s: %v", subscriptionArn, err)

			return "", err
		}
	} else {
		subscriptionArn, err = s.getSnsSqsSubscriptionArn(ctx, topicArn)
		if err != nil {
			s.logger.Errorf("error fetching info for topic ARN %s: %w", topicArn, err)

			return "", err
		}
	}

	s.subscriptions[compositeKey] = subscriptionArn
	s.logger.Debugf("Subscribed to topic %s: %s", topicArn, subscriptionArn)

	return subscriptionArn, nil
}

func (s *snsSqs) acknowledgeMessage(parentCtx context.Context, queueURL string, receiptHandle *string) error {
	ctx, cancelFn := context.WithCancel(parentCtx)
	_, err := s.sqsClient.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: receiptHandle,
	})
	cancelFn()
	if err != nil {
		return fmt.Errorf("error deleting message: %w", err)
	}

	return nil
}

func (s *snsSqs) resetMessageVisibilityTimeout(parentCtx context.Context, queueURL string, receiptHandle *string) error {
	ctx, cancelFn := context.WithCancel(parentCtx)
	// reset the timeout to its initial value so that the remaining timeout would be overridden by the initial value for other consumer to attempt processing.
	_, err := s.sqsClient.ChangeMessageVisibilityWithContext(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queueURL),
		ReceiptHandle:     receiptHandle,
		VisibilityTimeout: aws.Int64(0),
	})
	cancelFn()
	if err != nil {
		return fmt.Errorf("error changing message visibility timeout: %w", err)
	}

	return nil
}

func (s *snsSqs) parseReceiveCount(message *sqs.Message) (int64, error) {
	// if this message has been received > x times, delete from queue, it's borked.
	recvCount, ok := message.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]

	if !ok {
		return 0, fmt.Errorf(
			"no ApproximateReceiveCount returned with response, will not attempt further processing: %v", message)
	}

	recvCountInt, err := strconv.ParseInt(*recvCount, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("error parsing ApproximateReceiveCount from message: %v", message)
	}
	return recvCountInt, nil
}

func (s *snsSqs) validateMessage(ctx context.Context, message *sqs.Message, queueInfo, deadLettersQueueInfo *sqsQueueInfo) error {
	recvCount, err := s.parseReceiveCount(message)
	if err != nil {
		return err
	}

	messageRetryLimit := s.metadata.MessageRetryLimit
	if deadLettersQueueInfo == nil && recvCount >= messageRetryLimit {
		// if we are over the allowable retry limit, and there is no dead-letters queue, and we don't disable deletes, then delete the message from the queue.
		if !s.metadata.DisableDeleteOnRetryLimit {
			if innerErr := s.acknowledgeMessage(ctx, queueInfo.url, message.ReceiptHandle); innerErr != nil {
				return fmt.Errorf("error acknowledging message after receiving the message too many times: %w", innerErr)
			}
			return fmt.Errorf("message received greater than %v times, deleting this message without further processing", messageRetryLimit)
		}
		// if we are over the allowable retry limit, and there is no dead-letters queue, and deletes are disabled, then don't delete the message from the queue.
		// reset the already "consumed" message visibility clock.
		s.logger.Debugf("message received greater than %v times. deletion past the thredhold is diabled. noop", messageRetryLimit)
		if err := s.resetMessageVisibilityTimeout(ctx, queueInfo.url, message.ReceiptHandle); err != nil {
			return fmt.Errorf("error resetting message visibility timeout: %w", err)
		}

		return nil
	}

	// ... else, there is no need to actively do something if we reached the limit defined in messageReceiveLimit as the message had
	// already been moved to the dead-letters queue by SQS. meaning, the below condition should not be reached as SQS would not send
	// a message if we've already surpassed the messageRetryLimit value.
	if deadLettersQueueInfo != nil && recvCount > messageRetryLimit {
		awsErr := fmt.Errorf(
			"message received greater than %v times, this message should have been moved without further processing to dead-letters queue: %v", messageRetryLimit, s.metadata.SqsDeadLettersQueueName)

		return awsErr
	}

	return nil
}

func (s *snsSqs) callHandler(ctx context.Context, message *sqs.Message, queueInfo *sqsQueueInfo) error {
	// otherwise, try to handle the message.
	var snsMessagePayload snsMessage
	err := json.Unmarshal([]byte(*(message.Body)), &snsMessagePayload)
	if err != nil {
		return fmt.Errorf("error unmarshalling message: %w", err)
	}

	// snsMessagePayload.TopicArn can only carry a sanitized topic name as we conform to AWS naming standards.
	// for the user to be able to understand the source of the coming message, we'd use the original,
	// dirty name to be carried over in the pubsub.NewMessage Topic field.
	sanitizedTopic := snsMessagePayload.parseTopicArn()
	// get a handler by sanitized topic name and perform validations
	var (
		handler *SubscriptionTopicHandler
		loadOK  bool
	)
	if handler, loadOK = s.subscriptionManager.GetSubscriptionTopicHandler(sanitizedTopic); loadOK {
		if len(handler.requestTopic) == 0 {
			return errors.New("handler topic name is missing")
		}
	} else {
		return fmt.Errorf("handler for (sanitized) topic: %s was not found", sanitizedTopic)
	}

	s.logger.Debugf("Processing SNS message id: %s of (sanitized) topic: %s", *message.MessageId, sanitizedTopic)

	// call the handler with its own subscription context
	err = handler.handler(handler.ctx, &pubsub.NewMessage{
		Data:  []byte(snsMessagePayload.Message),
		Topic: handler.requestTopic,
	})
	if err != nil {
		return fmt.Errorf("error handling message: %w", err)
	}
	// otherwise, there was no error, acknowledge the message.
	return s.acknowledgeMessage(ctx, queueInfo.url, message.ReceiptHandle)
}

// consumeSubscription is responsible for polling messages from the queue and calling the handler.
// it is being passed as a callback to the subscription manager that initializes the context of the handler.
func (s *snsSqs) consumeSubscription(ctx context.Context, queueInfo, deadLettersQueueInfo *sqsQueueInfo) {
	sqsPullExponentialBackoff := s.backOffConfig.NewBackOffWithContext(ctx)

	receiveMessageInput := &sqs.ReceiveMessageInput{
		// use this property to decide when a message should be discarded.
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameApproximateReceiveCount),
		},
		MaxNumberOfMessages: aws.Int64(s.metadata.MessageMaxNumber),
		QueueUrl:            aws.String(queueInfo.url),
		VisibilityTimeout:   aws.Int64(s.metadata.MessageVisibilityTimeout),
		WaitTimeSeconds:     aws.Int64(s.metadata.MessageWaitTimeSeconds),
	}

	for {
		// If the context is canceled, stop requesting messages
		if ctx.Err() != nil {
			break
		}

		// Internally, by default, aws go sdk performs 3 retires with exponential backoff to contact
		// sqs and try pull messages. Since we are iteratively short polling (based on the defined
		// s.metadata.messageWaitTimeSeconds) the sdk backoff is not effective as it gets reset per each polling
		// iteration. Therefore, a global backoff (to the internal backoff) is used (sqsPullExponentialBackoff).
		messageResponse, err := s.sqsClient.ReceiveMessageWithContext(ctx, receiveMessageInput)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				s.logger.Warn("context canceled; stopping consuming from queue arn: %v", queueInfo.arn)
				continue
			}

			var awsErr awserr.Error
			if errors.As(err, &awsErr) {
				s.logger.Errorf("AWS operation error while consuming from queue arn: %v with error: %w. retrying...", queueInfo.arn, awsErr.Error())
			} else {
				s.logger.Errorf("error consuming from queue arn: %v with error: %w. retrying...", queueInfo.arn, err)
			}
			time.Sleep(sqsPullExponentialBackoff.NextBackOff())

			continue
		}
		// error either recovered or did not happen at all. resetting the backoff counter (and duration).
		sqsPullExponentialBackoff.Reset()

		if len(messageResponse.Messages) < 1 {
			continue
		}
		s.logger.Debugf("%v message(s) received on queue %s", len(messageResponse.Messages), queueInfo.arn)

		var wg sync.WaitGroup
		for _, message := range messageResponse.Messages {
			if err := s.validateMessage(ctx, message, queueInfo, deadLettersQueueInfo); err != nil {
				s.logger.Errorf("message is not valid for further processing by the handler. error is: %v", err)
				continue
			}

			f := func(message *sqs.Message) {
				defer wg.Done()
				if err := s.callHandler(ctx, message, queueInfo); err != nil {
					s.logger.Errorf("error while handling received message. error is: %v", err)
				}
			}

			wg.Add(1)
			switch s.metadata.ConcurrencyMode {
			case pubsub.Single:
				f(message)
			case pubsub.Parallel:
				wg.Add(1)
				go func(message *sqs.Message) {
					defer wg.Done()
					f(message)
				}(message)
			}
		}
		wg.Wait()
	}
}

func (s *snsSqs) createDeadLettersQueueAttributes(queueInfo, deadLettersQueueInfo *sqsQueueInfo) (*sqs.SetQueueAttributesInput, error) {
	policy := map[string]string{
		"deadLetterTargetArn": deadLettersQueueInfo.arn,
		"maxReceiveCount":     strconv.FormatInt(s.metadata.MessageReceiveLimit, 10),
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

func (s *snsSqs) setDeadLettersQueueAttributes(parentCtx context.Context, queueInfo, deadLettersQueueInfo *sqsQueueInfo) error {
	if s.metadata.DisableEntityManagement {
		return nil
	}

	var sqsSetQueueAttributesInput *sqs.SetQueueAttributesInput
	sqsSetQueueAttributesInput, derr := s.createDeadLettersQueueAttributes(queueInfo, deadLettersQueueInfo)
	if derr != nil {
		wrappedErr := fmt.Errorf("error creating queue attributes for dead-letter queue: %w", derr)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	ctx, cancelFn := context.WithTimeout(parentCtx, s.opsTimeout)
	_, derr = s.sqsClient.SetQueueAttributesWithContext(ctx, sqsSetQueueAttributesInput)
	cancelFn()
	if derr != nil {
		wrappedErr := fmt.Errorf("error updating queue attributes with dead-letter queue: %w", derr)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	return nil
}

func (s *snsSqs) restrictQueuePublishPolicyToOnlySNS(parentCtx context.Context, sqsQueueInfo *sqsQueueInfo, snsARN string) error {
	// not creating any policies of disableEntityManagement is true.
	if s.metadata.DisableEntityManagement {
		return nil
	}

	ctx, cancelFn := context.WithTimeout(parentCtx, s.opsTimeout)
	// only permit SNS to send messages to SQS using the created subscription.
	getQueueAttributesOutput, err := s.sqsClient.GetQueueAttributesWithContext(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &sqsQueueInfo.url,
		AttributeNames: []*string{aws.String(sqs.QueueAttributeNamePolicy)},
	})
	cancelFn()
	if err != nil {
		return fmt.Errorf("error getting queue attributes: %w", err)
	}

	policy := &policy{Version: "2012-10-17"}
	if policyStr, ok := getQueueAttributesOutput.Attributes[sqs.QueueAttributeNamePolicy]; ok {
		// look for the current statement if exists, else add it and store.
		if err = json.Unmarshal([]byte(*policyStr), policy); err != nil {
			return fmt.Errorf("error unmarshalling sqs policy: %w", err)
		}
	}
	conditionExists := policy.tryInsertCondition(sqsQueueInfo.arn, snsARN)
	if conditionExists {
		return nil
	}

	b, uerr := json.Marshal(policy)
	if uerr != nil {
		return fmt.Errorf("failed serializing new sqs policy: %w", uerr)
	}

	ctx, cancelFn = context.WithTimeout(parentCtx, s.opsTimeout)
	_, err = s.sqsClient.SetQueueAttributesWithContext(ctx, &(sqs.SetQueueAttributesInput{
		Attributes: map[string]*string{
			"Policy": aws.String(string(b)),
		},
		QueueUrl: &sqsQueueInfo.url,
	}))
	cancelFn()
	if err != nil {
		return fmt.Errorf("error setting queue subscription policy: %w", err)
	}

	return nil
}

func (s *snsSqs) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if s.closed.Load() {
		return errors.New("component is closed")
	}

	// subscribers declare a topic ARN and declare a SQS queue to use
	// these should be idempotent - queues should not be created if they exist.
	topicArn, sanitizedName, err := s.getOrCreateTopic(ctx, req.Topic)
	if err != nil {
		wrappedErr := fmt.Errorf("error getting topic ARN for %s: %w", req.Topic, err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	// this is the ID of the application, it is supplied via runtime as "consumerID".
	var queueInfo *sqsQueueInfo
	queueInfo, err = s.getOrCreateQueue(ctx, s.metadata.SqsQueueName)
	if err != nil {
		wrappedErr := fmt.Errorf("error retrieving SQS queue: %w", err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	// only after a SQS queue and SNS topic had been setup, we restrict the SendMessage action to SNS as sole source
	// to prevent anyone but SNS to publish message to SQS.
	err = s.restrictQueuePublishPolicyToOnlySNS(ctx, queueInfo, topicArn)
	if err != nil {
		wrappedErr := fmt.Errorf("error setting sns-sqs subscription policy: %w", err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	// apply the dead letters queue attributes to the current queue.
	var deadLettersQueueInfo *sqsQueueInfo
	var derr error

	if len(s.metadata.SqsDeadLettersQueueName) > 0 {
		deadLettersQueueInfo, derr = s.getOrCreateQueue(ctx, s.metadata.SqsDeadLettersQueueName)
		if derr != nil {
			wrappedErr := fmt.Errorf("error retrieving SQS dead-letter queue: %w", err)
			s.logger.Error(wrappedErr)

			return wrappedErr
		}

		err = s.setDeadLettersQueueAttributes(ctx, queueInfo, deadLettersQueueInfo)
		if err != nil {
			wrappedErr := fmt.Errorf("error creating dead-letter queue: %w", err)
			s.logger.Error(wrappedErr)

			return wrappedErr
		}
	}

	// subscription creation is idempotent. Subscriptions are unique by topic/queue.
	_, err = s.getOrCreateSnsSqsSubscription(ctx, queueInfo.arn, topicArn)
	if err != nil {
		wrappedErr := fmt.Errorf("error subscribing topic: %s, to queue: %s, with error: %w", topicArn, queueInfo.arn, err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	// start the subscription manager
	s.subscriptionManager.Init(queueInfo, deadLettersQueueInfo, s.consumeSubscription)

	s.subscriptionManager.Subscribe(&SubscriptionTopicHandler{
		topic:        sanitizedName,
		requestTopic: req.Topic,
		handler:      handler,
		ctx:          ctx,
	})

	return nil
}

func (s *snsSqs) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if s.closed.Load() {
		return errors.New("component is closed")
	}

	topicArn, _, err := s.getOrCreateTopic(ctx, req.Topic)
	if err != nil {
		s.logger.Errorf("error getting topic ARN for %s: %v", req.Topic, err)
	}

	message := string(req.Data)
	snsPublishInput := &sns.PublishInput{
		Message:  aws.String(message),
		TopicArn: aws.String(topicArn),
	}
	if s.metadata.Fifo {
		snsPublishInput.MessageGroupId = s.getMessageGroupID(req)
	}

	// sns client has internal exponential backoffs.
	_, err = s.snsClient.PublishWithContext(ctx, snsPublishInput)
	if err != nil {
		wrappedErr := fmt.Errorf("error publishing to topic: %s with topic ARN %s: %w", req.Topic, topicArn, err)
		s.logger.Error(wrappedErr)

		return wrappedErr
	}

	return nil
}

// Close should always be called to release the resources used by the SNS/SQS
// client. Blocks until all goroutines have returned.
func (s *snsSqs) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		s.subscriptionManager.Close()
	}

	return nil
}

func (s *snsSqs) Features() []pubsub.Feature {
	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (s *snsSqs) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := snsSqsMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.PubSubType)
	return
}
