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
