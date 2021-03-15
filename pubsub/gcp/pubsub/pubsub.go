package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	gcppubsub "cloud.google.com/go/pubsub"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	errorMessagePrefix                 = "gcp pubsub error:"
	metadataConsumerIDKey              = "consumerID"
	metadataTypeKey                    = "type"
	metadataProjectIDKey               = "projectId"
	metadataIdentityProjectIDKey       = "identityProjectId"
	metadataPrivateKeyIdKey            = "privateKeyId"
	metadataClientEmailKey             = "clientEmail"
	metadataClientIdKey                = "clientId"
	metadataAuthUriKey                 = "authUri"
	metadataTokenUriKey                = "tokenUri"
	metadataAuthProviderX509CertUrlKey = "authProviderX509CertUrl"
	metadataClientX509CertUrlKey       = "clientX509CertUrl"
	metadataPrivateKeyKey              = "privateKey"
	metadataDisableEntityManagementKey = "disableEntityManagement"
)

// GCPPubSub type
type GCPPubSub struct {
	client   *gcppubsub.Client
	metadata *metadata
	logger   logger.Logger
}

type GCPAuthJson struct {
	ProjectID           string `json:"project_id"`
	PrivateKeyID        string `json:"private_key_id"`
	PrivateKey          string `json:"private_key"`
	ClientEmail         string `json:"client_email"`
	ClientID            string `json:"client_id"`
	AuthURI             string `json:"auth_uri"`
	TokenURI            string `json:"token_uri"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url"`
	Type                string `json:"type"`
}

type WhatNow struct {
	Type string `json:"type"`
}

// NewGCPPubSub returns a new GCPPubSub instance
func NewGCPPubSub(logger logger.Logger) pubsub.PubSub {
	return &GCPPubSub{logger: logger}
}

func createMetadata(pubSubMetadata pubsub.Metadata) (*metadata, error) {
	// TODO: Add the rest of the metadata here, add defaults where applicable
	result := metadata{
		DisableEntityManagement: false,
		Type:                    "service_account",
	}

	if val, found := pubSubMetadata.Properties[metadataTypeKey]; found && val != "" {
		result.Type = val
	}

	if val, found := pubSubMetadata.Properties[metadataConsumerIDKey]; found && val != "" {
		result.consumerID = val
	}

	if val, found := pubSubMetadata.Properties[metadataIdentityProjectIDKey]; found && val != "" {
		result.IdentityProjectID = val
	}

	if val, found := pubSubMetadata.Properties[metadataProjectIDKey]; found && val != "" {
		result.ProjectID = val
	} else {
		return &result, fmt.Errorf("%s missing attribute %s", errorMessagePrefix, metadataProjectIDKey)
	}

	if val, found := pubSubMetadata.Properties[metadataPrivateKeyIdKey]; found && val != "" {
		result.PrivateKeyID = val
	}

	if val, found := pubSubMetadata.Properties[metadataClientEmailKey]; found && val != "" {
		result.ClientEmail = val
	}


	if val, found := pubSubMetadata.Properties[metadataClientIdKey]; found && val != "" {
		result.ClientID = val
	}

	if val, found := pubSubMetadata.Properties[metadataAuthUriKey]; found && val != "" {
		result.AuthURI = val
	}

	if val, found := pubSubMetadata.Properties[metadataTokenUriKey]; found && val != "" {
		result.TokenURI = val
	}

	if val, found := pubSubMetadata.Properties[metadataAuthProviderX509CertUrlKey]; found && val != "" {
		result.AuthProviderCertURL = val
	}

	if val, found := pubSubMetadata.Properties[metadataClientX509CertUrlKey]; found && val != "" {
		result.ClientCertURL = val
	}

	if val, found := pubSubMetadata.Properties[metadataPrivateKeyKey]; found && val != "" {
		result.PrivateKey = val
	}

	if val, found := pubSubMetadata.Properties[metadataDisableEntityManagementKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.DisableEntityManagement = boolVal
		}
	}
	return &result, nil
}

// Init parses metadata and creates a new Pub Sub client
func (g *GCPPubSub) Init(meta pubsub.Metadata) error {
	//meta, err := createMetadata(meta)
	myMeta, err := createMetadata(meta)
	if err != nil {
		return err
	}
	b, err := g.parseMetadata(myMeta)
	//g.logger.Debugf(string(b))
	if err != nil {
		return err
	}

	var pubsubMeta metadata
	err = json.Unmarshal(b, &pubsubMeta)
	if err != nil {
		return err
	}

	ctx := context.Background()
	pubsubClient, err := g.getPubSubClient(myMeta, ctx)
	if err != nil {
		return err
	}

	if err != nil {
		return fmt.Errorf("%s error creating pubsub client: %s", errorMessagePrefix, err)
	}

	if val, ok := meta.Properties[metadataConsumerIDKey]; ok && val != "" {
		pubsubMeta.consumerID = val
	} else {
		return fmt.Errorf("%s missing consumerID", errorMessagePrefix)
	}

	g.client = pubsubClient
	g.metadata = &pubsubMeta

	return nil
}

func (g *GCPPubSub) getPubSubClient(metadata *metadata, ctx context.Context) (*gcppubsub.Client, error) {
	pubsubClient, err := gcppubsub.NewClient(ctx, metadata.ProjectID)
	if err != nil {
		return pubsubClient, err
	}

	if metadata.PrivateKeyID != "" {
		//TODO: validate that all auth json fields are filled
		authJson := &GCPAuthJson{
			ProjectID:           metadata.IdentityProjectID,
			PrivateKeyID:        metadata.PrivateKeyID,
			PrivateKey:          metadata.PrivateKey,
			ClientEmail:         metadata.ClientEmail,
			ClientID:            metadata.ClientID,
			AuthURI:             metadata.AuthURI,
			TokenURI:            metadata.TokenURI,
			AuthProviderCertURL: metadata.AuthProviderCertURL,
			ClientCertURL:       metadata.ClientCertURL,
			Type:                metadata.Type,
		}
		gcpCompatibleJson, _ := json.Marshal(authJson)
		g.logger.Debugf("Using explicit credentials for GCP")
		clientOptions := option.WithCredentialsJSON(gcpCompatibleJson)
		pubsubClient, err = gcppubsub.NewClient(ctx, metadata.ProjectID, clientOptions)
		if err != nil {
			return pubsubClient, err
		}
	} else {
		g.logger.Debugf("Using implicit credentials for GCP")
	}
	return pubsubClient, nil
}

func (g *GCPPubSub) parseMetadata(metadata *metadata) ([]byte, error) {
	b, err := json.Marshal(metadata)

	return b, err
}

// Publish the topic to GCP Pubsub
func (g *GCPPubSub) Publish(req *pubsub.PublishRequest) error {
	if !g.metadata.DisableEntityManagement {
		err := g.ensureTopic(req.Topic)
		if err != nil {
			return fmt.Errorf("%s could not get valid topic %s, %s", errorMessagePrefix, req.Topic, err)
		}
	}

	ctx := context.Background()
	topic := g.getTopic(req.Topic)

	_, err := topic.Publish(ctx, &gcppubsub.Message{
		Data: req.Data,
	}).Get((ctx))

	return err
}

// Subscribe to the GCP Pubsub topic
func (g *GCPPubSub) Subscribe(req pubsub.SubscribeRequest, daprHandler func(msg *pubsub.NewMessage) error) error {
	if !g.metadata.DisableEntityManagement {
		topicErr := g.ensureTopic(req.Topic)
		if topicErr != nil {
			return fmt.Errorf("%s could not get valid topic %s, %s", errorMessagePrefix, req.Topic, topicErr)
		}

		subError := g.ensureSubscription(g.metadata.consumerID, req.Topic)
		if subError != nil {
			return fmt.Errorf("%s could not get valid subscription %s, %s", errorMessagePrefix, g.metadata.consumerID, subError)
		}
	}

	topic := g.getTopic(req.Topic)
	sub := g.getSubscription(g.metadata.consumerID + "-" + req.Topic)

	go g.handleSubscriptionMessages(topic, sub, daprHandler)

	return nil
}

func (g *GCPPubSub) handleSubscriptionMessages(topic *gcppubsub.Topic, sub *gcppubsub.Subscription, daprHandler func(msg *pubsub.NewMessage) error) error {
	err := sub.Receive(context.Background(), func(ctx context.Context, m *gcppubsub.Message) {
		msg := &pubsub.NewMessage{
			Data:  m.Data,
			Topic: topic.ID(),
		}

		err := daprHandler(msg)

		if err == nil {
			m.Ack()
		}
	})

	return err
}

func (g *GCPPubSub) ensureTopic(topic string) error {
	entity := g.getTopic(topic)
	exists, err := entity.Exists(context.Background())
	if err != nil {
		return err
	}

	if !exists {
		_, err = g.client.CreateTopic(context.Background(), topic)
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}

		return err
	}

	return nil
}

func (g *GCPPubSub) getTopic(topic string) *gcppubsub.Topic {
	return g.client.Topic(topic)
}

func (g *GCPPubSub) ensureSubscription(subscription string, topic string) error {
	err := g.ensureTopic(topic)
	if err != nil {
		return err
	}

	managedSubscription := subscription + "-" + topic
	entity := g.getSubscription(managedSubscription)
	exists, subErr := entity.Exists(context.Background())
	if !exists {
		_, subErr = g.client.CreateSubscription(context.Background(), managedSubscription,
			gcppubsub.SubscriptionConfig{Topic: g.getTopic(topic)})
	}

	return subErr
}

func (g *GCPPubSub) getSubscription(subscription string) *gcppubsub.Subscription {
	return g.client.Subscription(subscription)
}

func (g *GCPPubSub) Close() error {
	return g.client.Close()
}

func (g *GCPPubSub) Features() []pubsub.Feature {
	return nil
}
