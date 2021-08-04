package snssqs

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {

	code := m.Run()
	os.Exit(code)
}

type testFixture struct {
	topicName            string
	deadLettersQueueName string
	queueName            string
}

func getFixture() *testFixture {
	return &testFixture{
		topicName:            "dapr-sns-test-topic",
		deadLettersQueueName: "dapr-sqs-test-deadletters-queue",
		queueName:            "dapr-sqs-test-queue",
	}
}

func newAWSSession(cfg *s3UploaderConfig) *session.Session {
	var mySession *session.Session
    sessionCfg := aws.NewConfig()
	// Create a S3 client with additional configuration
	if len(cfg.EndpointURL) != 0 {
		sessionCfg.Endpoint = &cfg.EndpointURL
		sessionCfg.Region = &cfg.Region
		sessionCfg.Credentials = credentials.NewStaticCredentials("my-key", "my-secret", "")
		sessionCfg.DisableSSL = aws.Bool(true)

		opts := session.Options{}
		opts.Profile = "minio"
		opts.Config = *sessionCfg
		mySession = session.Must(session.NewSessionWithOptions(opts))
	} else {
		sessionCfg.Region = aws.String(endpoints.UsEast1RegionID)
		mySession = session.Must(session.NewSession(sessionCfg))
	}
	return mySession
}

func setupTestCase(t *testing.T) func(t *testing.T) {
	t.Log("setup test case")
	return func(t *testing.T) {
		t.Log("teardown test case")
	}
}

func snsSqsTest(t *testing.T) func(t *testing.T) {
	fixture := getFixture()
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		
	}))

	t.Log("setup sub test")

	snssqsClient := NewSnsSqs(logger.NewLogger("test"))
	err := snssqsClient.Init(pubsub.Metadata{
		Properties: map[string]string{
			"region":     os.Getenv("AWS_DEFAULT_REGION"),
			"accessKey":  os.Getenv("AWS_ACCESS_KEY_ID"),
			"secretKey":  os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"endpoint":   os.Getenv("AWS_ENDPOINT_URL"),
			"consumerID": fixture.queueName,
		},
	})

	// subscriber listens to SQS queue
	req := pubsub.SubscribeRequest{Topic: fixture.queueName}
	msgs := make([]*pubsub.NewMessage, 1)
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		msgs = append(msgs, msg)

		return nil
	}

	err = snssqsClient.Subscribe(req, handler)
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

		svc := sqs.New(sess)
		_, err := svc.DeleteQueue(&sqs.DeleteQueueInput{
			QueueUrl: queueURL.QueueUrl,
		})

		assert.Nil(t, err)

		snsSvc := sns.New(sess)
		result, err := snsSvc.ListSubscriptions(nil)
		if err != nil {
			fmt.Println(err.Error())

			os.Exit(1)
		}

		for _, s := range result.Subscriptions {
			fmt.Println(*s.SubscriptionArn)
			fmt.Println("  " + *s.TopicArn)
			fmt.Println("")
		}

		t.Log("teardown sub test")

	}
}

func TestSnsSqs(t *testing.T) {
	cases := []struct {
		name     string
		a        int
		b        int
		expected int
	}{
		{"add", 2, 2, 4},
		{"minus", 0, -2, -2},
		{"zero", 0, 0, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			teardownSnsSqsTest := snsSqsTest(t)
			defer teardownSnsSqsTest(t)

			// result := Sum(tc.a, tc.b)
			// if result != tc.expected {
			//     t.Fatalf("expected sum %v, but got %v", tc.expected, result)
			// }
		})
	}
}

func getQueueURL(sess *session.Session, queueName *string) (*sqs.GetQueueUrlOutput, error) {
	// Create an SQS service client
	svc := sqs.New(sess)

	endpointAccountId := "000000000000"
	result, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName:              queueName,
		QueueOwnerAWSAccountId: &endpointAccountId,
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// TestIntegrationGetSecret requires AWS specific environments for authentication AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KkEY and AWS_SESSION_TOKEN
func TestIntegrationCreateAllSnsSqs(t *testing.T) {

	// assert.Nil(t, err)
	// assert.NotNil(t, response)
}
