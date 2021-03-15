package pubsub

import "github.com/dapr/components-contrib/pubsub"

// GCPPubSubMetaData pubsub metadata
type metadata struct {
	consumerID              string
	DisableEntityManagement bool
	Type                    string
	ProjectID               string
	PrivateKeyID            string
	PrivateKey              string
	ClientEmail             string
	ClientID                string
	AuthURI                 string
	TokenURI                string
	AuthProviderCertURL     string
	ClientCertURL           string
}


func createMetadata(pubSubMetadata pubsub.Metadata) (*metadata, error) {
	// TODO: Add the rest of the metadata here, add defaults where applicable
	result := metadata{
		DisableEntityManagement: true,
		Type: "service_account",
	}

	if val, found := pubSubMetadata.Properties[metadataTypeKey]; found && val != "" {
		result.Type = val
	}

	if val, found := pubSubMetadata.Properties[metadataConsumerIDKey]; found && val != "" {
		result.consumerID = val
	}

	return &result, nil
}