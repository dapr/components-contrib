package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

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
	//json format expected by gcp for explicit auth
	project_id                  string
	private_key_id              string
	private_key                 string
	client_email                string
	client_id                   string
	auth_uri                    string
	token_uri                   string
	auth_provider_x509_cert_url string
	client_x509_cert_url        string
}

// NewGCPPubSub returns a new GCPPubSub instance
func NewGCPPubSub(logger logger.Logger) pubsub.PubSub {
	return &GCPPubSub{logger: logger}
}

// Init parses metadata and creates a new Pub Sub client
func (g *GCPPubSub) Init(meta pubsub.Metadata) error {
	//meta, err := createMetadata(meta)
	myMeta, err := createMetadata(meta)
	b, err := g.parseMetadata(myMeta)
	g.logger.Debugf(string(b))
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
		// explicit credentials
		authJson := GCPAuthJson{
			project_id:                  metadata.ProjectID,
			private_key_id:              metadata.PrivateKeyID,
			private_key:                 metadata.PrivateKey,
			client_email:                metadata.ClientEmail,
			client_id:                   metadata.ClientID,
			auth_uri:                    metadata.AuthURI,
			token_uri:                   metadata.TokenURI,
			auth_provider_x509_cert_url: metadata.AuthProviderCertURL,
			client_x509_cert_url:        metadata.ClientCertURL,
		}
		gcpCompatibleJson, err := json.Marshal(authJson)
		if err != nil {
			return pubsubClient, err
		}
		clientOptions := option.WithCredentialsJSON(gcpCompatibleJson)
		pubsubClient, err = gcppubsub.NewClient(ctx, metadata.ProjectID, clientOptions)
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
