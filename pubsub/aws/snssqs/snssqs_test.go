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

	r.Equal("consumer", md.sqsQueueName)
	r.Equal("endpoint", md.Endpoint)
	r.Equal(pubsub.Single, md.concurrencyMode)
	r.Equal("a", md.AccessKey)
	r.Equal("s", md.SecretKey)
	r.Equal("t", md.SessionToken)
	r.Equal("r", md.Region)
	r.Equal("q", md.sqsDeadLettersQueueName)
	r.Equal(int64(2), md.messageVisibilityTimeout)
	r.Equal(int64(3), md.messageRetryLimit)
	r.Equal(int64(4), md.messageWaitTimeSeconds)
	r.Equal(int64(5), md.messageMaxNumber)
	r.Equal(int64(6), md.messageReceiveLimit)
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

	r.Equal("c", md.sqsQueueName)
	r.Equal("", md.Endpoint)
	r.Equal("a", md.AccessKey)
	r.Equal("s", md.SecretKey)
	r.Equal("", md.SessionToken)
	r.Equal("r", md.Region)
	r.Equal(pubsub.Parallel, md.concurrencyMode)
	r.Equal(int64(10), md.messageVisibilityTimeout)
	r.Equal(int64(10), md.messageRetryLimit)
	r.Equal(int64(2), md.messageWaitTimeSeconds)
	r.Equal(int64(10), md.messageMaxNumber)
	r.Equal(false, md.disableEntityManagement)
	r.Equal(float64(5), md.assetsManagementTimeoutSeconds)
	r.Equal(false, md.disableDeleteOnRetryLimit)
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

	r.Equal("consumer", md.sqsQueueName)
	r.Equal("", md.Endpoint)
	r.Equal("acctId", md.AccessKey)
	r.Equal("secret", md.SecretKey)
	r.Equal("region", md.Region)
	r.Equal(int64(10), md.messageVisibilityTimeout)
	r.Equal(int64(10), md.messageRetryLimit)
	r.Equal(int64(2), md.messageWaitTimeSeconds)
	r.Equal(int64(10), md.messageMaxNumber)
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
				"consumerID": "consumer",
				"Endpoint":   "endpoint",
				"AccessKey":  "acctId",
				"SecretKey":  "secret",
				"awsToken":   "token",
				"Region":     "region",
				"fifo":       "none bool",
			}}},
			name: "fifo not set to boolean",
		},
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
			name: "illigal message max number (negative, too low)",
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
			name: "illigal message max number (too high)",
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
		// disableEntityManagement
		{
			metadata: pubsub.Metadata{Base: metadata.Base{Properties: map[string]string{
				"consumerID":              "consumer",
				"Endpoint":                "endpoint",
				"AccessKey":               "acctId",
				"SecretKey":               "secret",
				"awsToken":                "token",
				"Region":                  "region",
				"messageRetryLimit":       "10",
				"disableEntityManagement": "y",
			}}},
			name: "invalid message disableEntityManagement",
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

	// Expecting that this function doesn't panic.
	_, err = parseInt64("999999999999999999999999999999999999999999999999999999999999999999999999999", "")
	r.Error(err)
}

func Test_replaceNameToAWSSanitizedName(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `Some_invalid-name // for an AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^Some invalid 
		name // for an AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^Some invalid name // for an
		AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^`
	v := nameToAWSSanitizedName(s, false)
	r.Equal(80, len(v))
	r.Equal("Some_invalid-nameforanAWSresourceSomeinvalidnameforanAWSresourceSomeinvalidnamef", v)
}

func Test_replaceNameToAWSSanitizedFifoName_Trimmed(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `Some_invalid-name // for an AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^Some invalid 
		name // for an AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^Some invalid name // for an
		AWS resource &*()*&&^Some invalid name // for an AWS resource &*()*&&^`
	v := nameToAWSSanitizedName(s, true)
	r.Equal(80, len(v))
	r.Equal("Some_invalid-nameforanAWSresourceSomeinvalidnameforanAWSresourceSomeinvalid.fifo", v)
}

func Test_replaceNameToAWSSanitizedFifoName_NonTrimmed(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `012345678901234567890123456789012345678901234567890123456789012345678901234`
	v := nameToAWSSanitizedName(s, true)
	r.Equal(80, len(v))
	r.Equal("012345678901234567890123456789012345678901234567890123456789012345678901234.fifo", v)
}

func Test_replaceNameToAWSSanitizedExistingFifoName_NonTrimmed(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `012345678901234567890123456789012345678901234567890123456789012345678901234.fifo`
	v := nameToAWSSanitizedName(s, true)
	r.Equal(80, len(v))
	r.Equal("012345678901234567890123456789012345678901234567890123456789012345678901234.fifo", v)
}

func Test_replaceNameToAWSSanitizedExistingFifoName_NonMax(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `0123456789`
	v := nameToAWSSanitizedName(s, true)
	r.Equal(len(s)+len(".fifo"), len(v))
	r.Equal("0123456789.fifo", v)
}

func Test_replaceNameToAWSSanitizedExistingFifoName_NoFifoSetting(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `012345678901234567890123456789012345678901234567890123456789012345678901234.fifo`
	v := nameToAWSSanitizedName(s, false)
	r.Equal(79, len(v))
	r.Equal("012345678901234567890123456789012345678901234567890123456789012345678901234fifo", v)
}

func Test_replaceNameToAWSSanitizedExistingFifoName_Trimmed(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `01234567890123456789012345678901234567890123456789012345678901234567890123456789.fifo`
	v := nameToAWSSanitizedName(s, true)
	r.Equal(80, len(v))
	r.Equal("012345678901234567890123456789012345678901234567890123456789012345678901234.fifo", v)
}

func Test_UnmarshalJSON_UnmarshallsToArray(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	s := `
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
        "ForAllValues:ArnEquals": {
          "aws:SourceArn": "snsArn"
        }
      }
    }
  ]
}
`
	p := &policy{}

	err := json.Unmarshal([]byte(s), p)
	r.Equal(err, nil)

	statement := p.Statement[0]
	r.Equal(len(statement.Condition.ForAllValuesArnEquals.AwsSourceArn), 1)
	r.Equal(statement.Condition.ForAllValuesArnEquals.AwsSourceArn[0], "snsArn")
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

	r.Equal(len(policy.Statement), 1)
	insertedStatement := policy.Statement[0]
	r.Equal(insertedStatement.Resource, sqsArn)
	r.Equal(len(insertedStatement.Condition.ForAllValuesArnEquals.AwsSourceArn), len(snsArns))
	r.ElementsMatch(insertedStatement.Condition.ForAllValuesArnEquals.AwsSourceArn, snsArns)
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
	r.Equal(err, nil)

	policy.tryInsertCondition(sqsArn, snsArn)
	r.Equal(len(policy.Statement), 1)
	insertedStatement := policy.Statement[0]
	r.Equal(insertedStatement.Resource, sqsArn)
	r.Equal(len(insertedStatement.Condition.ForAllValuesArnEquals.AwsSourceArn), 1)
	r.Equal(insertedStatement.Condition.ForAllValuesArnEquals.AwsSourceArn[0], snsArn)
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
	md.accountID = "123456789012"
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
	md.accountID = "123456789012"
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
	md.accountID = "123456789012"
	ps.metadata = md

	arn := ps.buildARN("sns", "myTopic")
	r.Equal("arn:aws-cn:sns:cn-northwest-1:123456789012:myTopic", arn)
}
