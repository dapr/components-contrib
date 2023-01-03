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
	"fmt"

	// AWS SDK
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func deleteQueues(queues []string) error {
	svc := sqsService()
	for _, queue := range queues {
		if err := deleteQueue(svc, queue); err != nil {
			return fmt.Errorf("error deleting the queue URL: %q err:%v", queue, err)
		}
	}
	return nil
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

func deleteQueue(svc *sqs.SQS, queue string) error {
	queueUrlOutput, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queue,
	})
	if err != nil {
		return fmt.Errorf("error getting the queue URL: %q err:%v", queue, err)
	}

	_, err = svc.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: queueUrlOutput.QueueUrl,
	})

	return err
}
