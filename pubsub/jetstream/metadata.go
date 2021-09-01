package jetstream

import (
	"fmt"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/pubsub"
)

type metadata struct {
	natsURL string

	name           string
	durableName    string
	queueGroupName string
	startSequence  uint64
	startTime      time.Time
	deliverAll     bool
	flowControl    bool
}

func parseMetadata(psm pubsub.Metadata) (metadata, error) {
	var m metadata

	if v, ok := psm.Properties["natsURL"]; ok && v != "" {
		m.natsURL = v
	} else {
		return metadata{}, fmt.Errorf("missing nats URL")
	}

	if m.name = psm.Properties["name"]; m.name == "" {
		m.name = "dapr.io - pubsub.jetstream"
	}

	m.durableName = psm.Properties["durableName"]
	m.queueGroupName = psm.Properties["queueGroupName"]

	if v, err := strconv.ParseUint(psm.Properties["startSequence"], 10, 64); err == nil {
		m.startSequence = v
	}

	if v, err := strconv.ParseInt(psm.Properties["startTime"], 10, 64); err == nil {
		m.startTime = time.Unix(v, 0)
	}

	if v, err := strconv.ParseBool(psm.Properties["deliverAll"]); err == nil {
		m.deliverAll = v
	}

	if v, err := strconv.ParseBool(psm.Properties["flowControl"]); err == nil {
		m.flowControl = v
	}

	return m, nil
}
