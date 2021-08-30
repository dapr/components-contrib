package pubsub

// GCPPubSubMetaData pubsub metadata
type metadata struct {
	consumerID              string
	Type                    string
	IdentityProjectID       string
	ProjectID               string
	PrivateKeyID            string
	PrivateKey              string
	ClientEmail             string
	ClientID                string
	AuthURI                 string
	TokenURI                string
	AuthProviderCertURL     string
	ClientCertURL           string
	DisableEntityManagement bool
	EnableMessageOrdering   bool
}
