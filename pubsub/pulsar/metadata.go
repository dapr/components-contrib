package pulsar

type pulsarMetadata struct {
	Host            string `json:"host"`
	ConsumerID      string `json:"consumerID"`
	EnableTLS       bool   `json:"enableTLS"`
	DisableBatching bool   `json:"disableBatching"`
	Tenant          string `json:"tenant"`
	Namespace       string `json:"namespace"`
	Persistent      bool   `json:"persistent"`
}
