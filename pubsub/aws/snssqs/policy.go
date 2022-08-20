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
	"reflect"
)

type arnEquals struct {
	AwsSourceArn awsSourceArn `json:"aws:SourceArn"`
}

type awsSourceArn []string

// UnmarshalJSON This is a custom unmarshaler for awsSourceArn for handling a special case
// where aws flatten awsSourceArn into a string when it only contains one element
func (a *awsSourceArn) UnmarshalJSON(data []byte) error {
	var i interface{}
	err := json.Unmarshal(data, &i)
	if err != nil {
		return err
	}

	items := reflect.ValueOf(i)
	switch items.Kind() {
	case reflect.String:
		*a = append(*a, items.String())
	case reflect.Slice:
		*a = make([]string, 0, items.Len())
		for i := 0; i < items.Len(); i++ {
			item := items.Index(i)
			switch item.Kind() {
			case reflect.String:
				*a = append(*a, item.String())
			case reflect.Interface:
				*a = append(*a, item.Interface().(string))
			}
		}
	}
	return nil
}

type condition struct {
	ForAllValuesArnEquals arnEquals `json:"ForAllValues:ArnEquals"`
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
	for i, s := range p.Statement {
		// if there is a statement for sqsArn
		if s.Resource == sqsArn {
			// check if the snsArn already exists
			for _, a := range s.Condition.ForAllValuesArnEquals.AwsSourceArn {
				if a == snsArn {
					return true
				}
			}
			// insert it if it does not exist
			p.Statement[i].Condition.ForAllValuesArnEquals.AwsSourceArn = append(p.Statement[i].Condition.ForAllValuesArnEquals.AwsSourceArn, snsArn)
			return false
		}
	}
	// insert a new statement if no statement for the sqsArn
	newStatement := &statement{
		Effect:    "Allow",
		Principal: principal{Service: "sns.amazonaws.com"},
		Action:    "sqs:SendMessage",
		Resource:  sqsArn,
		Condition: condition{
			ForAllValuesArnEquals: arnEquals{
				AwsSourceArn: []string{snsArn},
			},
		},
	}
	p.Statement = append(p.Statement, *newStatement)
	return false
}
