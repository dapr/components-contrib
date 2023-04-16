/*
Copyright 2022 The Dapr Authors
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

package snssqs_test

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/sts/stsiface"
)

var (
	partition   string = "aws"
	serviceName string = "sns"
)

func deleteQueues(queues []string) error {
	svc := sqsService()
	for _, queue := range queues {
		if err := deleteQueue(svc, queue); err != nil {
			fmt.Printf("error deleting the queue URL: %q err:%v", queue, err)
		}
	}
	return nil
}

func deleteQueue(svc *sqs.SQS, queue string) error {
	fmt.Printf("deleteQueue: %q\n", queue)
	queueUrl, err := getQueueURL(svc, queue)
	if err != nil {
		return fmt.Errorf("error getting the queue URL: %q err:%v", queue, err)
	}

	_, err = svc.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: &queueUrl,
	})

	return err
}

func getQueueURL(svc *sqs.SQS, queue string) (string, error) {
	urlResult, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queue),
	})

	if err != nil {
		return "", err
	}

	return *urlResult.QueueUrl, nil
}

func getMessages(svc *sqs.SQS, queueURL string) (*sqs.ReceiveMessageOutput, error) {
	input := sqs.ReceiveMessageInput{
		// use this property to decide when a message should be discarded.
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameApproximateReceiveCount),
		},
		MaxNumberOfMessages: aws.Int64(10),
		QueueUrl:            aws.String(queueURL),
		VisibilityTimeout:   aws.Int64(5),
		WaitTimeSeconds:     aws.Int64(20),
	}

	msgResult, err := svc.ReceiveMessage(&input)
	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

func deleteMessage(svc *sqs.SQS, queueURL, messageHandle string) error {
	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(messageHandle),
	})
	if err != nil {
		return err
	}

	return nil
}

func deleteTopics(topics []string, region string) error {
	sess := session.Must(
		session.NewSessionWithOptions(
			session.Options{
				SharedConfigState: session.SharedConfigEnable,
			},
		))
	svc := sns.New(sess)
	id, err := getIdentity(sts.New(sess))
	if err != nil {
		return err
	}

	for _, topic := range topics {
		topicArn := buildARN(partition, serviceName, topic, region, id)
		fmt.Printf("Getting subscriptions for topicArn: %s\n", topicArn)
		if subout, err := svc.ListSubscriptionsByTopic(&sns.ListSubscriptionsByTopicInput{
			TopicArn: aws.String(topicArn),
		}); err == nil {
			for _, sub := range subout.Subscriptions {
				if err := unsubscribeFromTopic(svc, *sub.SubscriptionArn); err != nil {
					fmt.Printf("error unsubscribing arn: %q err:%v\n", *sub.SubscriptionArn, err)
				}
			}
		} else {
			fmt.Printf("error getting subscription list topic: %q err:%v\n", topic, err)
		}

		if err := deleteTopic(svc, topicArn); err != nil {
			fmt.Printf("error deleting the topic: %q err:%v\n", topic, err)
		}
	}
	return nil
}

func deleteTopic(svc snsiface.SNSAPI, topic string) error {
	fmt.Printf("deleteTopic: %q\n", topic)
	_, err := svc.DeleteTopic(&sns.DeleteTopicInput{
		TopicArn: aws.String(topic),
	})

	return err
}

func unsubscribeFromTopic(svc snsiface.SNSAPI, subscription string) error {
	_, err := svc.Unsubscribe(&sns.UnsubscribeInput{
		SubscriptionArn: aws.String(subscription),
	})

	return err
}

func sqsService() *sqs.SQS {
	sess := session.Must(
		session.NewSessionWithOptions(
			session.Options{
				SharedConfigState: session.SharedConfigEnable,
			},
		))
	return sqs.New(sess)
}

func getIdentity(svc stsiface.STSAPI) (*sts.GetCallerIdentityOutput, error) {
	input := &sts.GetCallerIdentityInput{}
	result, err := svc.GetCallerIdentity(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				return nil, fmt.Errorf(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			return nil, fmt.Errorf(aerr.Error())
		}
		return nil, err
	}

	return result, nil
}

func buildARN(partition, serviceName, entityName, region string, id *sts.GetCallerIdentityOutput) string {
	return fmt.Sprintf("arn:%s:%s:%s:%s:%s", partition, serviceName, region, *id.Account, entityName)
}

type QueueManager struct {
	svc *sqs.SQS
}

type SNSMessagePayload struct {
	Message  string
	TopicArn string
}
type DataMessage struct {
	Data string `json:"data"`
}

type MessageFunc func(*DataMessage) error

func NewQueueManager() *QueueManager {
	qm := QueueManager{}
	qm.connect()
	return &qm
}

func (qm *QueueManager) connect() error {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	qm.svc = sqs.New(sess)
	return nil
}

func (qm *QueueManager) GetMessages(queue string, deleteMsg bool, mf MessageFunc) (int, error) {
	queueURL, err := getQueueURL(qm.svc, queue)
	if err != nil {
		return -1, err
	}

	msgResult, err := getMessages(qm.svc, queueURL)
	if err != nil {
		return -1, err
	}

	numMgs := len(msgResult.Messages)
	for _, msg := range msgResult.Messages {
		dm, err := extractDataMessage(msg)
		if err != nil {
			return -1, err
		}

		if err := mf(dm); err != nil {
			return -1, err
		}
		if deleteMsg {
			err = deleteMessage(qm.svc, queueURL, *msg.ReceiptHandle)
			if err != nil {
				return -1, err
			}
		}
	}

	return numMgs, nil
}

func extractDataMessage(msg *sqs.Message) (*DataMessage, error) {
	snsMP := SNSMessagePayload{}
	err := json.Unmarshal([]byte(*(msg.Body)), &snsMP)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling message Body: %v", err)
	}
	dm := DataMessage{}
	err = json.Unmarshal([]byte(snsMP.Message), &dm)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling message data: %v", err)
	}

	return &dm, nil
}
