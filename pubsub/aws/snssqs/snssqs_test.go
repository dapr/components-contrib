package snssqs

import (
	"testing"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/require"
)

func Test_parseTopicArn(t *testing.T) {
	t.Parallel()
	// no further guarantees are made about this function
	r := require.New(t)
	r.Equal("qqnoob", parseTopicArn("arn:aws:sqs:us-east-1:000000000000:qqnoob"))
}

// Verify that all metadata ends up in the correct spot
func Test_getSnsSqsMetatdata_AllConfiguration(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID":               "consumer",
		"Endpoint":                 "endpoint",
		"accessKey":                "a",
		"secretKey":                "s",
		"sessionToken":             "t",
		"region":                   "r",
		"messageVisibilityTimeout": "2",
		"messageRetryLimit":        "3",
		"messageWaitTimeSeconds":   "4",
		"messageMaxNumber":         "5",
	}})

	r.NoError(err)

	r.Equal("consumer", md.sqsQueueName)
	r.Equal("endpoint", md.Endpoint)
	r.Equal("a", md.AccessKey)
	r.Equal("s", md.SecretKey)
	r.Equal("t", md.SessionToken)
	r.Equal("r", md.Region)
	r.Equal(int64(2), md.messageVisibilityTimeout)
	r.Equal(int64(3), md.messageRetryLimit)
	r.Equal(int64(4), md.messageWaitTimeSeconds)
	r.Equal(int64(5), md.messageMaxNumber)
}

func Test_getSnsSqsMetatdata_defaults(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID": "c",
		"accessKey":  "a",
		"secretKey":  "s",
		"region":     "r",
	}})

	r.NoError(err)

	r.Equal("c", md.sqsQueueName)
	r.Equal("", md.Endpoint)
	r.Equal("a", md.AccessKey)
	r.Equal("s", md.SecretKey)
	r.Equal("", md.SessionToken)
	r.Equal("r", md.Region)
	r.Equal(int64(10), md.messageVisibilityTimeout)
	r.Equal(int64(10), md.messageRetryLimit)
	r.Equal(int64(1), md.messageWaitTimeSeconds)
	r.Equal(int64(10), md.messageMaxNumber)
}

func Test_getSnsSqsMetatdata_legacyaliases(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID":   "consumer",
		"awsAccountID": "acctId",
		"awsSecret":    "secret",
		"awsRegion":    "region",
	}})

	r.NoError(err)

	r.Equal("consumer", md.sqsQueueName)
	r.Equal("", md.Endpoint)
	r.Equal("acctId", md.AccessKey)
	r.Equal("secret", md.SecretKey)
	r.Equal("region", md.Region)
	r.Equal(int64(10), md.messageVisibilityTimeout)
	r.Equal(int64(10), md.messageRetryLimit)
	r.Equal(int64(1), md.messageWaitTimeSeconds)
	r.Equal(int64(10), md.messageMaxNumber)
}

func Test_getSnsSqsMetatdata_invalidMessageVisibility(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID":               "consumer",
		"Endpoint":                 "endpoint",
		"AccessKey":                "acctId",
		"SecretKey":                "secret",
		"awsToken":                 "token",
		"Region":                   "region",
		"messageVisibilityTimeout": "-100",
	}})

	r.Error(err)
	r.Nil(md)
}

func Test_getSnsSqsMetatdata_invalidMessageRetryLimit(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID":        "consumer",
		"Endpoint":          "endpoint",
		"AccessKey":         "acctId",
		"SecretKey":         "secret",
		"awsToken":          "token",
		"Region":            "region",
		"messageRetryLimit": "-100",
	}})

	r.Error(err)
	r.Nil(md)
}

func Test_getSnsSqsMetatdata_invalidWaitTimeSecondsTooLow(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID":             "consumer",
		"Endpoint":               "endpoint",
		"AccessKey":              "acctId",
		"SecretKey":              "secret",
		"awsToken":               "token",
		"Region":                 "region",
		"messageWaitTimeSeconds": "0",
	}})

	r.Error(err)
	r.Nil(md)
}

func Test_getSnsSqsMetatdata_invalidMessageMaxNumberTooHigh(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID":       "consumer",
		"Endpoint":         "endpoint",
		"AccessKey":        "acctId",
		"SecretKey":        "secret",
		"awsToken":         "token",
		"Region":           "region",
		"messageMaxNumber": "100",
	}})

	r.Error(err)
	r.Nil(md)
}

func Test_getSnsSqsMetatdata_invalidMessageMaxNumberTooLow(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Properties: map[string]string{
		"consumerID":       "consumer",
		"Endpoint":         "endpoint",
		"AccessKey":        "acctId",
		"SecretKey":        "secret",
		"awsToken":         "token",
		"Region":           "region",
		"messageMaxNumber": "-100",
	}})

	r.Error(err)
	r.Nil(md)
}

func Test_parseInt64(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	number, err := parseInt64("applesauce", "propertyName")
	r.EqualError(err, "parsing propertyName failed with: strconv.Atoi: parsing \"applesauce\": invalid syntax")
	r.Equal(int64(-1), number)

	number, _ = parseInt64("1000", "")
	r.Equal(int64(1000), number)

	number, _ = parseInt64("-1000", "")
	r.Equal(int64(-1000), number)

	// Expecting that this function doesn't panic
	_, err = parseInt64("999999999999999999999999999999999999999999999999999999999999999999999999999", "")
	r.Error(err)
}

func Test_replaceNameToAWSSanitizedName(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `Some_invalid-name // for an AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^Some invalid 
		name // for an AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^Some invalid name // for an
		AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^`
	v := nameToAWSSanitizedName(s)
	r.Equal(80, len(v))
	r.Equal("Some_invalid-nameforanAWSresourceSomeinvalidnameforanAWSresourceSomeinvalidnamef", v)
}
