package pulsar

type pulsarMetadata struct {
	Host             string `json:"host"`
	SubscriptionName string `json:"subscriptionName"`
	EnableTLS        bool   `json:"enableTLS"`
}
