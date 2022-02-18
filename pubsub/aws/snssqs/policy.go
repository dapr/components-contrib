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

package snssqs

type arnEquals struct {
	AwsSourceArn string `json:"aws:SourceArn"`
}

type condition struct {
	ArnEquals arnEquals
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

func (p *policy) statementExists(other *statement) bool {
	for _, s := range p.Statement {
		if s.Effect == other.Effect &&
			s.Principal.Service == other.Principal.Service &&
			s.Action == other.Action &&
			s.Resource == other.Resource &&
			s.Condition.ArnEquals.AwsSourceArn == other.Condition.ArnEquals.AwsSourceArn {
			return true
		}
	}

	return false
}

func (p *policy) addStatement(other *statement) {
	p.Statement = append(p.Statement, *other)
}
