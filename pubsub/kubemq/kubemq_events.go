package kubemq

import (
	"context"
	"sync"
	"time"

	"github.com/kubemq-io/kubemq-go"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type kubemqEventsClient interface {
	Stream(ctx context.Context, onError func(err error)) (func(msg *kubemq.Event) error, error)
	Subscribe(ctx context.Context, request *kubemq.EventsSubscription, onEvent func(msg *kubemq.Event, err error)) error
	Close() error
}

type kubeMQEvents struct {
	lock                 sync.RWMutex
	client               kubemqEventsClient
	metadata             *metadata
	logger               logger.Logger
	publishFunc          func(event *kubemq.Event) error
	resultChan           chan error
	waitForResultTimeout time.Duration
	ctx                  context.Context
	ctxCancel            context.CancelFunc
	isInitialized        bool
}

func newkubeMQEvents(logger logger.Logger) *kubeMQEvents {
	return &kubeMQEvents{
		client:               nil,
		metadata:             nil,
		logger:               logger,
		publishFunc:          nil,
		resultChan:           make(chan error, 1),
		waitForResultTimeout: 60 * time.Second,
		ctx:                  nil,
		ctxCancel:            nil,
		isInitialized:        false,
	}
}

func (k *kubeMQEvents) init() error {
	k.lock.RLock()
	isInit := k.isInitialized
	k.lock.RUnlock()
	if isInit {
		return nil
	}
	k.lock.Lock()
	defer k.lock.Unlock()
	k.ctx, k.ctxCancel = context.WithCancel(context.Background())
	clientID := k.metadata.clientID
	if clientID == "" {
		clientID = getRandomID()
	}
	client, err := kubemq.NewEventsClient(k.ctx,
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

func (k *kubeMQEvents) Init(meta *metadata) error {
	k.metadata = meta
	_ = k.init()
	return nil
}

func (k *kubeMQEvents) setPublishStream() error {
	var err error
	k.publishFunc, err = k.client.Stream(k.ctx, func(err error) {
		select {
		case k.resultChan <- err:
		default:
		}
	})
	return err
}

func (k *kubeMQEvents) Publish(req *pubsub.PublishRequest) error {
	if err := k.init(); err != nil {
		return err
	}
	k.logger.Debugf("kubemq pub/sub: publishing message to %s", req.Topic)
	event := &kubemq.Event{
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
	return nil
}

func (k *kubeMQEvents) Features() []pubsub.Feature {
	return nil
}

func (k *kubeMQEvents) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if err := k.init(); err != nil {
		return err
	}
	clientID := k.metadata.clientID
	if clientID == "" {
		clientID = getRandomID()
	}
	k.logger.Debugf("kubemq pub/sub: subscribing to %s", req.Topic)
	err := k.client.Subscribe(ctx, &kubemq.EventsSubscription{
		Channel:  req.Topic,
		Group:    k.metadata.group,
		ClientId: clientID,
	}, func(event *kubemq.Event, err error) {
		if err != nil {
			k.logger.Errorf("kubemq pub/sub error: subscribing to %s failed with error: %s", req.Topic, err.Error())
			return
		}
		if ctx.Err() != nil {
			return
		}
		msg := &pubsub.NewMessage{
			Data:  event.Body,
			Topic: req.Topic,
		}

		if err := handler(k.ctx, msg); err != nil {
			k.logger.Errorf("kubemq events pub/sub error: error handling message from topic '%s', %s", req.Topic, err.Error())
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
		k.logger.Errorf("kubemq events pub/sub error: error subscribing to topic '%s', %s", req.Topic, err.Error())
		return err
	}
	time.Sleep(1 * time.Second)
	k.logger.Debugf("kubemq pub/sub: subscribed to %s completed", req.Topic)
	return nil
}

func (k *kubeMQEvents) Close() error {
	if k.ctxCancel != nil {
		k.ctxCancel()
	}
	return k.client.Close()
}
