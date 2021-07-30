package snssqs

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
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

func setupTestCase(t *testing.T) func(t *testing.T) {
	t.Log("setup test case")
	return func(t *testing.T) {
		t.Log("teardown test case")
	}
}

func snsSqsTest(t *testing.T) func(t *testing.T) {
	t.Log("setup sub test")
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	snsq := NewSnsSqs(logger.NewLogger("test"))
	err := snsq.Init(pubsub.Metadata{
		Properties: map[string]string{
			"Region":       os.Getenv("AWS_DEFAULT_REGION"),
			"AccessKey":    os.Getenv("AWS_ACCESS_KEY_ID"),
			"SecretKey":    os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"SessionToken": os.Getenv("AWS_SESSION_TOKEN"),
		},
	})

	fixture := getFixture()
	// subscriber listens to SQS queue
	req := pubsub.SubscribeRequest{Topic: fixture.queueName}
	handler := func(ctx context.Context, msg *pubsub.NewMessage) error {
		return nil
	}

	err = snsq.Subscribe(req, handler)
	assert.Nil(t, err)

	var topicArn *sqs.GetQueueUrlOutput
	topicArn, err = getQueueURL(sess, &fixture.queueName)
	assert.Nil(t, err)
	assert.NotNil(t, topicArn)

	// tear down callback
	return func(t *testing.T) {
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

	result, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: queueName,
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// TestIntegrationGetSecret requires AWS specific environments for authentication AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KkEY and AWS_SESSION_TOKEN
func TestIntegrationCreateAllSnsSqs(t *testing.T) {

	assert.Nil(t, err)
	// assert.NotNil(t, response)
}
