// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------
// +build e2e

package snssqs

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

type testFixture struct {
	name                 string
	endpoint             string
	region               string
	profile              string
	topicName            string
	deadLettersQueueName string
	messageReceiveLimit  string
	queueName            string
	accessKey            string
	secretKey            string
	sessionToken         string
}

func getDefaultTestFixture(messageReceiveLimits ...string) *testFixture {
	timestamp := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	fixture := &testFixture{
		region:       os.Getenv("AWS_DEFAULT_REGION"),
		accessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
		secretKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
		endpoint:     os.Getenv("AWS_ENDPOINT_URL"),
		profile:      os.Getenv("AWS_PROFILE"),
		sessionToken: os.Getenv("AWS_SESSION_TOKEN"),
		topicName:    fmt.Sprintf("dapr-sns-test-topic-%v", timestamp),
		queueName:    fmt.Sprintf("dapr-sqs-test-queue-%v", timestamp),
	}

	if len(messageReceiveLimits) > 0 {
		fixture.deadLettersQueueName = fmt.Sprintf("dapr-sqs-test-deadletters-queue-%v", timestamp)
		fixture.messageReceiveLimit = messageReceiveLimits[0]
	}

	return fixture
}

func newAWSSession(cfg *testFixture) *session.Session {
	// run localstack and use the endpoint url: http://localhost:4566 by using the following cmd
	// SERVICES=sns,sqs,sts DEBUG=1 localstack start
	var mySession *session.Session
	sessionCfg := aws.NewConfig()
	// Create a client with additional configuration
	if len(cfg.endpoint) != 0 {
		sessionCfg.Endpoint = &cfg.endpoint
		sessionCfg.Region = &cfg.region
		sessionCfg.Credentials = credentials.NewStaticCredentials(cfg.accessKey, cfg.secretKey, "")
		sessionCfg.DisableSSL = aws.Bool(true)

		opts := session.Options{Profile: cfg.profile, Config: *sessionCfg}
		mySession = session.Must(session.NewSessionWithOptions(opts))
	} else {
		sessionCfg.Region = aws.String(cfg.region)
		opts := session.Options{SharedConfigState: session.SharedConfigEnable, Config: *sessionCfg}
		mySession = session.Must(session.NewSessionWithOptions(opts))
	}

	return mySession
}

func setupTest(t *testing.T, fixture *testFixture) (pubsub.PubSub, *session.Session) {
	sess := newAWSSession(fixture)
	assert.NotNil(t, sess)
	t.Log("setup test")

	snssqsClient := NewSnsSqs(logger.NewLogger("test"))
	assert.NotNil(t, snssqsClient)

	props := map[string]string{
		"region":       fixture.region,
		"accessKey":    fixture.accessKey,
		"secretKey":    fixture.secretKey,
		"endpoint":     fixture.endpoint,
		"sessionToken": fixture.sessionToken,
		"consumerID":   fixture.queueName,
	}

	if len(fixture.deadLettersQueueName) > 0 {
		props["sqsDeadLettersQueueName"] = fixture.deadLettersQueueName
	}
	if len(fixture.messageReceiveLimit) > 0 {
		props["messageReceiveLimit"] = fixture.messageReceiveLimit
	}

	pubsubMetadata := pubsub.Metadata{Properties: props}

	err := snssqsClient.Init(pubsubMetadata)
	assert.Nil(t, err)

	return snssqsClient, sess
}

func getAccountID(sess client.ConfigProvider) (*sts.GetCallerIdentityOutput, error) {
	svc := sts.New(sess)
	input := &sts.GetCallerIdentityInput{}

	result, err := svc.GetCallerIdentity(input)
	if err != nil {
		return nil, err
	}

	return result, err
}

func getQueueURL(sess client.ConfigProvider, queueName *string) (*sqs.GetQueueUrlOutput, error) {
	// Get the account ID
	accountResult, aErr := getAccountID(sess)
	if aErr != nil {
		return nil, aErr
	}

	// Create an SQS service client
	svc := sqs.New(sess)
	result, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName:              queueName,
		QueueOwnerAWSAccountId: accountResult.Account,
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func teardownSqs(t *testing.T, sess client.ConfigProvider, fixture *testFixture) {
	svc := sqs.New(sess)

	queueURL, err := getQueueURL(sess, &fixture.queueName)
	assert.Nil(t, err)
	assert.NotNil(t, queueURL)

	_, err = svc.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: queueURL.QueueUrl,
	})
	assert.Nil(t, err)

	var dlQueueURL *sqs.GetQueueUrlOutput
	dlQueueURL, err = getQueueURL(sess, &fixture.deadLettersQueueName)
	// err would exist if no dead-letter queue exist, which might be the case
	// in some tests
	if err != nil {
		return
	}

	svc.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: dlQueueURL.QueueUrl,
	})
}

func teardownSns(t *testing.T, sess client.ConfigProvider, fixture *testFixture) {
	svc := sns.New(sess)
	result, err := svc.ListTopics(nil)
	assert.Nil(t, err)
	assert.NotNil(t, result)

	var accountID *sts.GetCallerIdentityOutput
	accountID, err = getAccountID(sess)
	assert.Nil(t, err)
	assert.NotNil(t, accountID)

	lookupTopicArn := fmt.Sprintf("arn:aws:sns:%v:%v:%v", fixture.region, *accountID.Account, fixture.topicName)
	for _, topic := range result.Topics {
		if *topic.TopicArn == lookupTopicArn {
			// deletes topic
			// currently there is a bug in aws-go-sdk that results in the subscription not being
			// deleted along with the topic (as should be) so the subscription needs
			// to be manually deleted
			_, err = svc.DeleteTopic(&sns.DeleteTopicInput{TopicArn: topic.TopicArn})
			assert.Nil(t, err)
		}
	}
}

func snsSqsTest(t *testing.T, sess client.ConfigProvider, snssqsClient pubsub.PubSub, fixture *testFixture) func(t *testing.T) {
	// subscriber registers to listen to (SNS) topic which eventually land on the fixture.queueName
	// over (SQS) queue
	req := pubsub.SubscribeRequest{Topic: fixture.topicName}
	msgs := []*pubsub.NewMessage{}
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		msgs = append(msgs, msg)

		return nil
	}

	err := snssqsClient.Subscribe(req, handler)
	assert.Nil(t, err)

	var queueURL *sqs.GetQueueUrlOutput
	queueURL, err = getQueueURL(sess, &fixture.queueName)
	assert.Nil(t, err)
	assert.NotNil(t, queueURL)

	publishReq := &pubsub.PublishRequest{Topic: fixture.topicName, PubsubName: "test", Data: []byte("string")}
	err = snssqsClient.Publish(publishReq)
	assert.Nil(t, err)
	// delay between send/recv in sqs
	time.Sleep(5 * time.Second)
	assert.Len(t, msgs, 1)

	// tear down callback
	return func(t *testing.T) {
		teardownSqs(t, sess, fixture)
		teardownSns(t, sess, fixture)

		t.Log("teardown test")
	}
}

func snsSqsDeadlettersTest(t *testing.T, sess client.ConfigProvider, snssqsClient pubsub.PubSub, fixture *testFixture) func(t *testing.T) {
	// subscriber's handlers always fails to process message forcing dead letters queue to
	req := pubsub.SubscribeRequest{Topic: fixture.topicName}
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		return fmt.Errorf("failure to receive - dead letters tests")
	}

	err := snssqsClient.Subscribe(req, handler)
	assert.Nil(t, err)

	var queueURL *sqs.GetQueueUrlOutput
	queueURL, err = getQueueURL(sess, &fixture.queueName)
	assert.Nil(t, err)
	assert.NotNil(t, queueURL)

	publishReq := &pubsub.PublishRequest{Topic: fixture.topicName, PubsubName: "test", Data: []byte("string")}
	err = snssqsClient.Publish(publishReq)
	assert.Nil(t, err)

	// tear down callback
	return func(t *testing.T) {
		sqsSvc := sqs.New(sess)
		dlQueueURL, err := getQueueURL(sess, &fixture.deadLettersQueueName)
		assert.Nil(t, err)

		waitTimeSeconds := int64(10)
		var output *sqs.ReceiveMessageOutput
		output, err = sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{QueueUrl: dlQueueURL.QueueUrl, WaitTimeSeconds: &waitTimeSeconds})
		assert.Nil(t, err)
		assert.NotNil(t, output.Messages)
		assert.Len(t, output.Messages, 1)

		teardownSqs(t, sess, fixture)
		teardownSns(t, sess, fixture)

		t.Log("teardown test")
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestSnsSqs(t *testing.T) {
	t.Parallel()

	fixture := getDefaultTestFixture()
	if (len(fixture.accessKey) == 0 && len(fixture.secretKey) == 0) && len(fixture.sessionToken) == 0 {
		t.Skip(
			`environment variables of either AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY 
			or AWS_SESSION_TOKEN must be set in order to run these integration
			tests
			`)
	}
	client, sess := setupTest(t, fixture)
	teardownSnsSqsTest := snsSqsTest(t, sess, client, fixture)
	defer teardownSnsSqsTest(t)
}

func TestSnsSqsWithDLQ(t *testing.T) {
	t.Parallel()

	fixture := getDefaultTestFixture("1")
	if (len(fixture.accessKey) == 0 && len(fixture.secretKey) == 0) && len(fixture.sessionToken) == 0 {
		t.Skip(
			`environment variables of either AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY 
			or AWS_SESSION_TOKEN must be set in order to run these integration
			tests
			`)
	}
	client, sess := setupTest(t, fixture)
	teardownSnsSqsTest := snsSqsDeadlettersTest(t, sess, client, fixture)
	defer teardownSnsSqsTest(t)
}
