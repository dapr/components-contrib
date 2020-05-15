package snssqs

import (
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_parseTopicArn(t *testing.T) {
	// no further guarantees are made about this function
	r := require.New(t)
	r.Equal("qqnoob", parseTopicArn("arn:aws:sqs:us-east-1:000000000000:qqnoob"))
}

func Test_snsSqs_getSnsSqsMetatdata_happyPath(t *testing.T) {
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID": "consumer",
		"awsEndpoint":  "endpoint",
		"awsAccountId":  "acctId",
		"awsSecret":  "secret",
		"awsToken":  "token",
		"awsRegion":  "region",
		"messageVisibilityTimeout":  "2",
		"messageRetryLimit":  "3",
		"messageWaitTimeSeconds":  "4",
		"messageMaxNumber":  "5",
	}})

	r.NoError(err)

	r.Equal("consumer", md.sqsQueueName)
	r.Equal("endpoint", md.awsEndpoint)
	r.Equal("acctId", md.awsAccountId)
	r.Equal("secret", md.awsSecret)
	r.Equal("token", md.awsToken)
	r.Equal("region", md.awsRegion)
	r.Equal(int64(2), md.messageVisibilityTimeout)
	r.Equal(int64(3), md.messageRetryLimit)
	r.Equal(int64(4), md.messageWaitTimeSeconds)
	r.Equal(int64(5), md.messageMaxNumber)

}

func Test_snsSqs_getSnsSqsMetatdata_defaults(t *testing.T) {
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID": "consumer",
		"awsAccountId":  "acctId",
		"awsSecret":  "secret",
		"awsToken":  "token",
		"awsRegion":  "region",
	}})

	r.NoError(err)

	r.Equal("consumer", md.sqsQueueName)
	r.Equal("", md.awsEndpoint)
	r.Equal("acctId", md.awsAccountId)
	r.Equal("secret", md.awsSecret)
	r.Equal("token", md.awsToken)
	r.Equal("region", md.awsRegion)
	r.Equal(int64(10), md.messageVisibilityTimeout)
	r.Equal(int64(10), md.messageRetryLimit)
	r.Equal(int64(1), md.messageWaitTimeSeconds)
	r.Equal(int64(10), md.messageMaxNumber)
}

func Test_snsSqs_getSnsSqsMetatdata_invalidMessageVisibility(t *testing.T) {
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID": "consumer",
		"awsEndpoint":  "endpoint",
		"awsAccountId":  "acctId",
		"awsSecret":  "secret",
		"awsToken":  "token",
		"awsRegion":  "region",
		"messageVisibilityTimeout":  "-100",
	}})

	r.Error(err)
	r.Nil(md)
}

func Test_snsSqs_getSnsSqsMetatdata_invalidMessageRetryLimit(t *testing.T) {
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID": "consumer",
		"awsEndpoint":  "endpoint",
		"awsAccountId":  "acctId",
		"awsSecret":  "secret",
		"awsToken":  "token",
		"awsRegion":  "region",
		"messageRetryLimit":  "-100",
	}})

	r.Error(err)
	r.Nil(md)
}

func Test_snsSqs_getSnsSqsMetatdata_invalidWaitTimeSecondsTooLow(t *testing.T) {
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID": "consumer",
		"awsEndpoint":  "endpoint",
		"awsAccountId":  "acctId",
		"awsSecret":  "secret",
		"awsToken":  "token",
		"awsRegion":  "region",
		"messageWaitTimeSeconds":  "0",
	}})

	r.Error(err)
	r.Nil(md)
}

func Test_snsSqs_getSnsSqsMetatdata_invalidMessageMaxNumberTooHigh(t *testing.T) {
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID": "consumer",
		"awsEndpoint":  "endpoint",
		"awsAccountId":  "acctId",
		"awsSecret":  "secret",
		"awsToken":  "token",
		"awsRegion":  "region",
		"messageMaxNumber":  "100",
	}})

	r.Error(err)
	r.Nil(md)
}

func Test_snsSqs_getSnsSqsMetatdata_invalidMessageMaxNumberTooLow(t *testing.T) {
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID": "consumer",
		"awsEndpoint":  "endpoint",
		"awsAccountId":  "acctId",
		"awsSecret":  "secret",
		"awsToken":  "token",
		"awsRegion":  "region",
		"messageMaxNumber":  "-100",
	}})

	r.Error(err)
	r.Nil(md)
}

func Test_snsSqs_parseInt64(t *testing.T) {
	r := require.New(t)
	number, err := parseInt64("applesauce", "propertyName")
	r.EqualError(err, "Parsing propertyName failed with: strconv.Atoi: parsing \"applesauce\": invalid syntax")
	r.Equal(int64(-1), number)

	number, err = parseInt64("1000", "")
	r.Equal(int64(1000), number)

	number, err = parseInt64("-1000", "")
	r.Equal(int64(-1000), number)

	// Expecting that this function doesn't panic
	number, err = parseInt64("999999999999999999999999999999999999999999999999999999999999999999999999999", "")
	r.Error(err)
}
