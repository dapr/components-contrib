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

package jetstream

import (
	"reflect"
	"testing"
	"time"

	"github.com/dapr/components-contrib/pubsub"
)

func TestParseMetadata(t *testing.T) {
	psm := pubsub.Metadata{
		Properties: map[string]string{
			"natsURL":        "nats://localhost:4222",
			"name":           "myName",
			"durableName":    "myDurable",
			"queueGroupName": "myQueue",
			"startSequence":  "1",
			"startTime":      "1629328511",
			"deliverAll":     "true",
			"flowControl":    "true",
		},
	}

	ts := time.Unix(1629328511, 0)

	want := metadata{
		natsURL:        "nats://localhost:4222",
		name:           "myName",
		durableName:    "myDurable",
		queueGroupName: "myQueue",
		startSequence:  1,
		startTime:      ts,
		deliverAll:     true,
		flowControl:    true,
	}

	got, err := parseMetadata(psm)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected metadata: got=%v, want=%v", got, want)
	}
}
