package pulsar

import "time"

type pulsarMetadata struct {
	Host         string        `json:"host"`
	ConsumerID   string        `json:"consumerID"`
	EnableTLS    bool          `json:"enableTLS"`
	DeliverAt    time.Time     `json:"deliverAt"`
	DeliverAfter time.Duration `json:"deliverAfter"`
}
