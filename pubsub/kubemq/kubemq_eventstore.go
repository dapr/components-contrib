package kubemq

import (
	"context"
	"fmt"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/kubemq-io/kubemq-go"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	"time"
)

// interface used to allow unit testing.
type kubemqEventsStoreClient interface {
	Stream(ctx context.Context, onResult func(result *kubemq.EventStoreResult, err error)) (func(msg *kubemq.EventStore) error, error)
	Subscribe(ctx context.Context, request *kubemq.EventsStoreSubscription, onEvent func(msg *kubemq.EventStoreReceive, err error)) error
	Close() error
}

type kubeMQEventStore struct {
	client               kubemqEventsStoreClient
	metadata             *metadata
	logger               logger.Logger
	publishFunc          func(msg *kubemq.EventStore) error
	resultChan           chan *kubemq.EventStoreResult
	waitForResultTimeout time.Duration
	ctx                  context.Context
	ctxCancel            context.CancelFunc
	isInitialized        bool
}

func newKubeMQEventsStore(logger logger.Logger) *kubeMQEventStore {
	return &kubeMQEventStore{
		client:               nil,
		metadata:             nil,
		logger:               logger,
		publishFunc:          nil,
		resultChan:           make(chan *kubemq.EventStoreResult, 1),
		waitForResultTimeout: 60 * time.Second,
		ctx:                  nil,
		ctxCancel:            nil,
	}
}
func (k *kubeMQEventStore) init() error {
	k.ctx, k.ctxCancel = context.WithCancel(context.Background())
	if k.metadata.useMock {
		k.isInitialized = true
		return nil
	}
	clientID := k.metadata.clientID
	if clientID == "" {
		clientID = uuid.New()
	}
	client, err := kubemq.NewEventsStoreClient(k.ctx,
		kubemq.WithAddress(k.metadata.host, k.metadata.port),
		kubemq.WithClientId(clientID),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithCheckConnection(true),
		kubemq.WithAuthToken(k.metadata.authToken),
		kubemq.WithAutoReconnect(true),
		kubemq.WithReconnectInterval(time.Second))
	if err != nil {
		k.logger.Errorf("error init kubemq client error: %s", err.Error())
		return err
	}
	k.ctx, k.ctxCancel = context.WithCancel(context.Background())
	k.client = client
	if err := k.setPublishStream(); err != nil {
		k.logger.Errorf("error init kubemq client error: %w", err.Error())
		return err
	}
	k.isInitialized = true
	return nil
}
func (k *kubeMQEventStore) getSubscriberClient(clientID string) (kubemqEventsStoreClient, error) {
	client, err := kubemq.NewEventsStoreClient(k.ctx,
		kubemq.WithAddress(k.metadata.host, k.metadata.port),
		kubemq.WithClientId(clientID),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithCheckConnection(true),
		kubemq.WithAuthToken(k.metadata.authToken),
		kubemq.WithAutoReconnect(true),
		kubemq.WithReconnectInterval(time.Second))
	if err != nil {
		k.logger.Errorf("error init kubemq client error: %s", err.Error())
		return nil, err
	}
	return client, nil
}

func (k *kubeMQEventStore) Init(meta *metadata) error {
	k.metadata = meta
	_ = k.init()
	return nil
}
func (k *kubeMQEventStore) setPublishStream() error {
	var err error
	k.publishFunc, err = k.client.Stream(k.ctx, func(result *kubemq.EventStoreResult, err error) {
		select {
		case k.resultChan <- result:
		default:
		}
	})
	if err != nil {
		return err
	}
	return nil
}
func (k *kubeMQEventStore) Publish(req *pubsub.PublishRequest) error {
	if !k.isInitialized {
		if err := k.init(); err != nil {
			return err
		}
	}
	k.logger.Debugf("kubemq pub/sub: publishing message to %s", req.Topic)
	event := &kubemq.EventStore{
		Id:       "",
		Channel:  req.Topic,
		Metadata: "",
		Body:     req.Data,
		ClientId: k.metadata.clientID,
		Tags:     map[string]string{},
	}
	if err := k.publishFunc(event); err != nil {
		k.logger.Errorf("kubemq pub/sub error: publishing to %s failed with error: %s", req.Topic, err.Error())
		return err
	}
	select {
	case res := <-k.resultChan:
		if res.Err != nil {
			return res.Err
		}
	case <-time.After(k.waitForResultTimeout):
		return fmt.Errorf("kubemq pub/sub error: timeout waiting for response")
	}
	return nil
}
func (k *kubeMQEventStore) Features() []pubsub.Feature {
	return nil
}

func (k *kubeMQEventStore) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if k.metadata.useMock {
		return nil
	}
	clientID := k.metadata.clientID
	if clientID == "" {
		clientID = uuid.New()
	}
	k.logger.Debugf("kubemq pub/sub: subscribing to %s", req.Topic)
	err := k.client.Subscribe(ctx, &kubemq.EventsStoreSubscription{
		Channel:          req.Topic,
		Group:            k.metadata.group,
		ClientId:         clientID,
		SubscriptionType: kubemq.StartFromNewEvents(),
	}, func(event *kubemq.EventStoreReceive, err error) {
		if err != nil {
			k.logger.Errorf("kubemq pub/sub error: subscribing to %s failed with error: %s", req.Topic, err.Error())
			return
		}
		if ctx.Err() != nil {
			return
		}
		msg := &pubsub.NewMessage{
			Data:        event.Body,
			Topic:       req.Topic,
			Metadata:    nil,
			ContentType: nil,
		}

		if err := handler(ctx, msg); err != nil {
			k.logger.Errorf("kubemq pub/sub error: error handling message from topic '%s', %s, resending...", req.Topic, err.Error())
			if k.metadata.disableReDelivery {
				return
			}
			if err := k.Publish(&pubsub.PublishRequest{
				Data:  msg.Data,
				Topic: msg.Topic,
			}); err != nil {
				k.logger.Errorf("kubemq pub/sub error: error resending message from topic '%s', %s", req.Topic, err.Error())
			}
		}

	})
	if err != nil {
		k.logger.Errorf("kubemq pub/sub error: error subscribing to topic '%s', %s", req.Topic, err.Error())
		return err
	}
	time.Sleep(1 * time.Second)
	k.logger.Debugf("kubemq pub/sub: subscribed to %s completed", req.Topic)
	return nil
}

func (k *kubeMQEventStore) Close() error {
	if k.ctxCancel != nil {
		k.ctxCancel()
	}
	return k.client.Close()
}
