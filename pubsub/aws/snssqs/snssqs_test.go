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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type testUnitFixture struct {
	metadata pubsub.Metadata
	name     string
}

func Test_parseTopicArn(t *testing.T) {
	t.Parallel()
	// no further guarantees are made about this function.
	r := require.New(t)
	tSnsMessage := &snsMessage{TopicArn: "arn:aws:sqs:us-east-1:000000000000:qqnoob"}
	r.Equal("qqnoob", tSnsMessage.parseTopicArn())
}

// Verify that all metadata ends up in the correct spot.
func Test_getSnsSqsMetatdata_AllConfiguration(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
		"consumerID":               "consumer",
		"Endpoint":                 "endpoint",
		"concurrencyMode":          string(pubsub.Single),
		"accessKey":                "a",
		"secretKey":                "s",
		"sessionToken":             "t",
		"region":                   "r",
		"sqsDeadLettersQueueName":  "q",
		"messageVisibilityTimeout": "2",
		"messageRetryLimit":        "3",
		"messageWaitTimeSeconds":   "4",
		"messageMaxNumber":         "5",
		"messageReceiveLimit":      "6",
	}}})

	r.NoError(err)

	r.Equal("consumer", md.SqsQueueName)
	r.Equal("endpoint", md.Endpoint)
	r.Equal(pubsub.Single, md.ConcurrencyMode)
	r.Equal("a", md.AccessKey)
	r.Equal("s", md.SecretKey)
	r.Equal("t", md.SessionToken)
	r.Equal("r", md.Region)
	r.Equal("q", md.SqsDeadLettersQueueName)
	r.Equal(int64(2), md.MessageVisibilityTimeout)
	r.Equal(int64(3), md.MessageRetryLimit)
	r.Equal(int64(4), md.MessageWaitTimeSeconds)
	r.Equal(int64(5), md.MessageMaxNumber)
	r.Equal(int64(6), md.MessageReceiveLimit)
}

func Test_getSnsSqsMetatdata_defaults(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
		"consumerID": "c",
		"accessKey":  "a",
		"secretKey":  "s",
		"region":     "r",
	}}})

	r.NoError(err)

	r.Equal("c", md.SqsQueueName)
	r.Equal("", md.Endpoint)
	r.Equal("a", md.AccessKey)
	r.Equal("s", md.SecretKey)
	r.Equal("", md.SessionToken)
	r.Equal("r", md.Region)
	r.Equal(pubsub.Parallel, md.ConcurrencyMode)
	r.Equal(int64(10), md.MessageVisibilityTimeout)
	r.Equal(int64(10), md.MessageRetryLimit)
	r.Equal(int64(2), md.MessageWaitTimeSeconds)
	r.Equal(int64(10), md.MessageMaxNumber)
	r.False(md.DisableEntityManagement)
	r.EqualValues(float64(5), md.AssetsManagementTimeoutSeconds)
	r.False(md.DisableDeleteOnRetryLimit)
}

func Test_getSnsSqsMetatdata_legacyaliases(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
		"consumerID":   "consumer",
		"awsAccountID": "acctId",
		"awsSecret":    "secret",
		"awsRegion":    "region",
	}}})

	r.NoError(err)

	r.Equal("consumer", md.SqsQueueName)
	r.Equal("", md.Endpoint)
	r.Equal("acctId", md.AccessKey)
	r.Equal("secret", md.SecretKey)
	r.Equal("region", md.Region)
	r.Equal(int64(10), md.MessageVisibilityTimeout)
	r.Equal(int64(10), md.MessageRetryLimit)
	r.Equal(int64(2), md.MessageWaitTimeSeconds)
	r.Equal(int64(10), md.MessageMaxNumber)
}

func testMetadataParsingShouldFail(t *testing.T, metadata pubsub.Metadata, l logger.Logger) {
	t.Parallel()
	r := require.New(t)

	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(metadata)

	r.Error(err)
	r.Nil(md)
}

func Test_getSnsSqsMetatdata_invalidMetadataSetup(t *testing.T) {
	t.Parallel()

	fixtures := []testUnitFixture{
		{
			metadata: pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
				"consumerID":          "consumer",
				"Endpoint":            "endpoint",
				"AccessKey":           "acctId",
				"SecretKey":           "secret",
				"awsToken":            "token",
				"Region":              "region",
				"messageReceiveLimit": "100",
			}}},
			name: "deadletters receive limit without deadletters queue name",
		},
		{
			metadata: pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
				"consumerID":              "consumer",
				"Endpoint":                "endpoint",
				"AccessKey":               "acctId",
				"SecretKey":               "secret",
				"awsToken":                "token",
				"Region":                  "region",
				"sqsDeadLettersQueueName": "my-queue",
			}}},
			name: "deadletters message queue without deadletters receive limit",
		},
		{
			metadata: pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
				"consumerID":                "consumer",
				"Endpoint":                  "endpoint",
				"AccessKey":                 "acctId",
				"SecretKey":                 "secret",
				"awsToken":                  "token",
				"Region":                    "region",
				"sqsDeadLettersQueueName":   "my-queue",
				"messageReceiveLimit":       "9",
				"disableDeleteOnRetryLimit": "true",
			}}},
			name: "deadletters message queue with disableDeleteOnRetryLimit",
		},
		{
			metadata: pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
				"consumerID":       "consumer",
				"Endpoint":         "endpoint",
				"AccessKey":        "acctId",
				"SecretKey":        "secret",
				"awsToken":         "token",
				"Region":           "region",
				"messageMaxNumber": "-100",
			}}},
			name: "illegal message max number (negative, too low)",
		},
		{
			metadata: pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
				"consumerID":       "consumer",
				"Endpoint":         "endpoint",
				"AccessKey":        "acctId",
				"SecretKey":        "secret",
				"awsToken":         "token",
				"Region":           "region",
				"messageMaxNumber": "100",
			}}},
			name: "illegal message max number (too high)",
		},
		{
			metadata: pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
				"consumerID":             "consumer",
				"Endpoint":               "endpoint",
				"AccessKey":              "acctId",
				"SecretKey":              "secret",
				"awsToken":               "token",
				"Region":                 "region",
				"messageWaitTimeSeconds": "0",
			}}},
			name: "invalid wait time seconds (too low)",
		},
		{
			metadata: pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
				"consumerID":               "consumer",
				"Endpoint":                 "endpoint",
				"AccessKey":                "acctId",
				"SecretKey":                "secret",
				"awsToken":                 "token",
				"Region":                   "region",
				"messageVisibilityTimeout": "-100",
			}}},
			name: "invalid message visibility",
		},
		{
			metadata: pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
				"consumerID":        "consumer",
				"Endpoint":          "endpoint",
				"AccessKey":         "acctId",
				"SecretKey":         "secret",
				"awsToken":          "token",
				"Region":            "region",
				"messageRetryLimit": "-100",
			}}},
			name: "invalid message retry limit",
		},
		// invalid concurrencyMode
		{
			metadata: pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
				"consumerID":        "consumer",
				"Endpoint":          "endpoint",
				"AccessKey":         "acctId",
				"SecretKey":         "secret",
				"awsToken":          "token",
				"Region":            "region",
				"messageRetryLimit": "10",
				"concurrencyMode":   "invalid",
			}}},
			name: "invalid message concurrencyMode",
		},
	}

	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)

	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			testMetadataParsingShouldFail(t, tc.metadata, l)
		})
	}
}

func Test_replaceNameToAWSSanitizedName(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `Some_invalid-name // for an AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^Some invalid 
		name // for an AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^Some invalid name // for an
		AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^`
	v := nameToAWSSanitizedName(s, false)
	r.Len(v, 80)
	r.Equal("Some_invalid-nameforanAWSresourceSomeinvalidnameforanAWSresourceSomeinvalidnamef", v)
}

func Test_replaceNameToAWSSanitizedFifoName_Trimmed(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `Some_invalid-name // for an AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^Some invalid 
		name // for an AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^Some invalid name // for an
		AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^`
	v := nameToAWSSanitizedName(s, true)
	r.Len(v, 80)
	r.Equal("Some_invalid-nameforanAWSresourceSomeinvalidnameforanAWSresourceSomeinvalid.fifo", v)
}

func Test_replaceNameToAWSSanitizedFifoName_NonTrimmed(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `012345678901234567890123456789012345678901234567890123456789012345678901234`
	v := nameToAWSSanitizedName(s, true)
	r.Len(v, 80)
	r.Equal("012345678901234567890123456789012345678901234567890123456789012345678901234.fifo", v)
}

func Test_replaceNameToAWSSanitizedExistingFifoName_NonTrimmed(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `012345678901234567890123456789012345678901234567890123456789012345678901234.fifo`
	v := nameToAWSSanitizedName(s, true)
	r.Len(v, 80)
	r.Equal("012345678901234567890123456789012345678901234567890123456789012345678901234.fifo", v)
}

func Test_replaceNameToAWSSanitizedExistingFifoName_NonMax(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `0123456789`
	v := nameToAWSSanitizedName(s, true)
	r.Len(v, len(s)+len(".fifo"))
	r.Equal("0123456789.fifo", v)
}

func Test_replaceNameToAWSSanitizedExistingFifoName_NoFifoSetting(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `012345678901234567890123456789012345678901234567890123456789012345678901234.fifo`
	v := nameToAWSSanitizedName(s, false)
	r.Len(v, 79)
	r.Equal("012345678901234567890123456789012345678901234567890123456789012345678901234fifo", v)
}

func Test_replaceNameToAWSSanitizedExistingFifoName_Trimmed(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `01234567890123456789012345678901234567890123456789012345678901234567890123456789.fifo`
	v := nameToAWSSanitizedName(s, true)
	r.Len(v, 80)
	r.Equal("012345678901234567890123456789012345678901234567890123456789012345678901234.fifo", v)
}

func Test_tryInsertCondition(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	policy := &policy{Version: "2012-10-17"}
	sqsArn := "sqsArn"
	snsArns := []string{"snsArns1", "snsArns2", "snsArns3", "snsArns4"}

	for _, snsArn := range snsArns {
		policy.tryInsertCondition(sqsArn, snsArn)
	}

	r.Len(policy.Statement, 4)
	insertedStatement := policy.Statement[0]
	r.Equal(insertedStatement.Resource, sqsArn)
	r.Equal(insertedStatement.Condition.ValueArnEquals.AwsSourceArn, snsArns[0])
	insertedStatement1 := policy.Statement[1]
	r.Equal(insertedStatement1.Resource, sqsArn)
	r.Equal(insertedStatement1.Condition.ValueArnEquals.AwsSourceArn, snsArns[1])
	insertedStatement2 := policy.Statement[2]
	r.Equal(insertedStatement2.Resource, sqsArn)
	r.Equal(insertedStatement2.Condition.ValueArnEquals.AwsSourceArn, snsArns[2])
	insertedStatement3 := policy.Statement[3]
	r.Equal(insertedStatement3.Resource, sqsArn)
	r.Equal(insertedStatement3.Condition.ValueArnEquals.AwsSourceArn, snsArns[3])
}

func Test_policy_compatible(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	sqsArn := "sqsArn"
	snsArn := "snsArn"
	oldPolicy := `
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "sns.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "sqsArn",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "snsArn"
        }
      }
    }
  ]
}
`
	policy := &policy{Version: "2012-10-17"}
	err := json.Unmarshal([]byte(oldPolicy), policy)
	r.NoError(err)

	policy.tryInsertCondition(sqsArn, snsArn)
	r.Len(policy.Statement, 1)
	insertedStatement := policy.Statement[0]
	r.Equal(insertedStatement.Resource, sqsArn)
	r.Equal(insertedStatement.Condition.ValueArnEquals.AwsSourceArn, snsArn)
}

func Test_buildARN_DefaultPartition(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
		"consumerID": "c",
		"accessKey":  "a",
		"secretKey":  "s",
		"region":     "r",
	}}})
	r.NoError(err)
	md.AccountID = "123456789012"
	ps.metadata = md

	arn := ps.buildARN("sns", "myTopic")
	r.Equal("arn:aws:sns:r:123456789012:myTopic", arn)
}

func Test_buildARN_StandardPartition(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
		"consumerID": "c",
		"accessKey":  "a",
		"secretKey":  "s",
		"region":     "us-west-2",
	}}})
	r.NoError(err)
	md.AccountID = "123456789012"
	ps.metadata = md

	arn := ps.buildARN("sns", "myTopic")
	r.Equal("arn:aws:sns:us-west-2:123456789012:myTopic", arn)
}

func Test_buildARN_NonStandardPartition(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	l := logger.NewLogger("SnsSqs unit test")
	l.SetOutputLevel(logger.DebugLevel)
	ps := snsSqs{
		logger: l,
	}

	md, err := ps.getSnsSqsMetatdata(pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
		"consumerID": "c",
		"accessKey":  "a",
		"secretKey":  "s",
		"region":     "cn-northwest-1",
	}}})
	r.NoError(err)
	md.AccountID = "123456789012"
	ps.metadata = md

	arn := ps.buildARN("sns", "myTopic")
	r.Equal("arn:aws-cn:sns:cn-northwest-1:123456789012:myTopic", arn)
}
