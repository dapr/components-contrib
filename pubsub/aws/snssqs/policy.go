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

type arnEquals struct {
	AwsSourceArn string `json:"aws:SourceArn"`
}

type condition struct {
	ValueArnEquals arnEquals `json:"ArnEquals"`
}

type principal struct {
	Service string
}

type statement struct {
	Effect    string
	Principal principal
	Action    string
	Resource  string
	Condition condition
}

type policy struct {
	Version   string
	Statement []statement
}

func (p *policy) tryInsertCondition(sqsArn string, snsArn string) bool {
	for _, s := range p.Statement {
		// if there is a statement for sqsArn
		if s.Resource == sqsArn {
			// check if the snsArn already exists then return true
			if s.Condition.ValueArnEquals.AwsSourceArn == snsArn {
				return true
			}
		}
	}
	// insert a new statement if no statement for the sqsArn or is new sns subscriber
	newStatement := &statement{
		Effect:    "Allow",
		Principal: principal{Service: "sns.amazonaws.com"},
		Action:    "sqs:SendMessage",
		Resource:  sqsArn,
		Condition: condition{
			ValueArnEquals: arnEquals{
				AwsSourceArn: snsArn,
			},
		},
	}
	p.Statement = append(p.Statement, *newStatement)
	return false
}
