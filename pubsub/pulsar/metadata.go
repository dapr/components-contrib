package pulsar

type pulsarMetadata struct {
	Host             string `json:"host"`
	SubscriptionName string `json:"subscriptionName"`
	ConsumerID       string `json:"consumerID"`
	EnableTLS        bool   `json:"enableTLS"`
}
