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

	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

func TestParseMetadata(t *testing.T) {
	testCases := []struct {
		desc      string
		input     pubsub.Metadata
		want      metadata
		expectErr bool
	}{
		{
			desc: "Valid Metadata",
			input: pubsub.Metadata{Base: mdata.Base{
				Properties: map[string]string{
					"natsURL":        "nats://localhost:4222",
					"name":           "myName",
					"durableName":    "myDurable",
					"queueGroupName": "myQueue",
					"startSequence":  "1",
					"startTime":      "1629328511",
					"deliverAll":     "true",
					"flowControl":    "true",
					"ackWait":        "2s",
					"maxDeliver":     "10",
					"backOff":        "500ms, 2s, 10s",
					"maxAckPending":  "5000",
					"replicas":       "3",
					"memoryStorage":  "true",
					"rateLimit":      "20000",
					"hearbeat":       "1s",
				},
			}},
			want: metadata{
				natsURL:        "nats://localhost:4222",
				name:           "myName",
				durableName:    "myDurable",
				queueGroupName: "myQueue",
				startSequence:  1,
				startTime:      time.Unix(1629328511, 0),
				deliverAll:     true,
				flowControl:    true,
				ackWait:        2 * time.Second,
				maxDeliver:     10,
				backOff:        []time.Duration{time.Millisecond * 500, time.Second * 2, time.Second * 10},
				maxAckPending:  5000,
				replicas:       3,
				memoryStorage:  true,
				rateLimit:      20000,
				hearbeat:       time.Second * 1,
			},
			expectErr: false,
		},
		{
			desc: "Valid Metadata with token",
			input: pubsub.Metadata{Base: mdata.Base{
				Properties: map[string]string{
					"natsURL":        "nats://localhost:4222",
					"name":           "myName",
					"durableName":    "myDurable",
					"queueGroupName": "myQueue",
					"startSequence":  "1",
					"startTime":      "1629328511",
					"deliverAll":     "true",
					"flowControl":    "true",
					"ackWait":        "2s",
					"maxDeliver":     "10",
					"backOff":        "500ms, 2s, 10s",
					"maxAckPending":  "5000",
					"replicas":       "3",
					"memoryStorage":  "true",
					"rateLimit":      "20000",
					"hearbeat":       "1s",
					"token":          "myToken",
				},
			}},
			want: metadata{
				natsURL:        "nats://localhost:4222",
				name:           "myName",
				durableName:    "myDurable",
				queueGroupName: "myQueue",
				startSequence:  1,
				startTime:      time.Unix(1629328511, 0),
				deliverAll:     true,
				flowControl:    true,
				ackWait:        2 * time.Second,
				maxDeliver:     10,
				backOff:        []time.Duration{time.Millisecond * 500, time.Second * 2, time.Second * 10},
				maxAckPending:  5000,
				replicas:       3,
				memoryStorage:  true,
				rateLimit:      20000,
				hearbeat:       time.Second * 1,
				token:          "myToken",
			},
			expectErr: false,
		},
		{
			desc: "Invalid metadata with missing seed key",
			input: pubsub.Metadata{Base: mdata.Base{
				Properties: map[string]string{
					"natsURL":        "nats://localhost:4222",
					"name":           "myName",
					"durableName":    "myDurable",
					"queueGroupName": "myQueue",
					"startSequence":  "1",
					"startTime":      "1629328511",
					"deliverAll":     "true",
					"flowControl":    "true",
					"jwt":            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
				},
			}},
			want:      metadata{},
			expectErr: true,
		},
		{
			desc: "Invalid metadata with missing jwt",
			input: pubsub.Metadata{Base: mdata.Base{
				Properties: map[string]string{
					"natsURL":        "nats://localhost:4222",
					"name":           "myName",
					"durableName":    "myDurable",
					"queueGroupName": "myQueue",
					"startSequence":  "1",
					"startTime":      "1629328511",
					"deliverAll":     "true",
					"flowControl":    "true",
					"seedKey":        "SUACS34K232OKPRDOMKC6QEWXWUDJTT6R6RZM2WPMURUS5Z3POU7BNIL4Y",
				},
			}},
			want:      metadata{},
			expectErr: true,
		},
		{
			desc: "Invalid metadata with missing tls client key",
			input: pubsub.Metadata{Base: mdata.Base{
				Properties: map[string]string{
					"natsURL":         "nats://localhost:4222",
					"name":            "myName",
					"durableName":     "myDurable",
					"queueGroupName":  "myQueue",
					"startSequence":   "1",
					"startTime":       "1629328511",
					"deliverAll":      "true",
					"flowControl":     "true",
					"tls_client_cert": "/path/to/tls.pem",
				},
			}},
			want:      metadata{},
			expectErr: true,
		},
		{
			desc: "Invalid metadata with missing tls client",
			input: pubsub.Metadata{Base: mdata.Base{
				Properties: map[string]string{
					"natsURL":        "nats://localhost:4222",
					"name":           "myName",
					"durableName":    "myDurable",
					"queueGroupName": "myQueue",
					"startSequence":  "1",
					"startTime":      "1629328511",
					"deliverAll":     "true",
					"flowControl":    "true",
					"tls_client_key": "/path/to/tls.key",
				},
			}},
			want:      metadata{},
			expectErr: true,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got, err := parseMetadata(tC.input)
			if !tC.expectErr && err != nil {
				t.Fatal(err)
			}
			if tC.expectErr && err == nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tC.want) {
				t.Fatalf("unexpected metadata: got=%v, want=%v", got, tC.want)
			}
		})
	}
}
