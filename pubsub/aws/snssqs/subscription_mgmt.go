package snssqs

import (
	"context"
	"sync"

	"github.com/puzpuzpuz/xsync/v3"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type (
	SubscriptionAction int
)

const (
	Subscribe SubscriptionAction = iota
	Unsubscribe
)

type SubscriptionTopicHandler struct {
	topic        string
	requestTopic string
	handler      pubsub.Handler
	ctx          context.Context
}

type changeSubscriptionTopicHandler struct {
	action  SubscriptionAction
	handler *SubscriptionTopicHandler
}

type SubscriptionManager struct {
	logger            logger.Logger
	consumeCancelFunc context.CancelFunc
	closeCh           chan struct{}
	topicsChangeCh    chan changeSubscriptionTopicHandler
	topicsHandlers    *xsync.MapOf[string, *SubscriptionTopicHandler]
	lock              sync.Mutex
	wg                sync.WaitGroup
	initOnce          sync.Once
}

type SubscriptionManagement interface {
	Init(queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo, cbk func(context.Context, *sqsQueueInfo, *sqsQueueInfo))
	Subscribe(topicHandler *SubscriptionTopicHandler)
	Close()
	GetSubscriptionTopicHandler(topic string) (*SubscriptionTopicHandler, bool)
}

func NewSubscriptionMgmt(log logger.Logger) SubscriptionManagement {
	return &SubscriptionManager{
		logger:            log,
		consumeCancelFunc: func() {}, // noop until we (re)start sqs consumption
		closeCh:           make(chan struct{}),
		topicsChangeCh:    make(chan changeSubscriptionTopicHandler),
		topicsHandlers:    xsync.NewMapOf[string, *SubscriptionTopicHandler](),
	}
}

func createQueueConsumerCbk(queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo, cbk func(ctx context.Context, queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo)) func(ctx context.Context) {
	return func(ctx context.Context) {
		cbk(ctx, queueInfo, dlqInfo)
	}
}

func (sm *SubscriptionManager) Init(queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo, cbk func(context.Context, *sqsQueueInfo, *sqsQueueInfo)) {
	sm.initOnce.Do(func() {
		queueConsumerCbk := createQueueConsumerCbk(queueInfo, dlqInfo, cbk)
		go sm.queueConsumerController(queueConsumerCbk)
		sm.logger.Debug("Subscription manager initialized")
	})
}

// queueConsumerController is responsible for managing the subscription lifecycle
// and the only place where the topicsHandlers map is updated.
// it is running in a separate goroutine and is responsible for starting and stopping sqs consumption
// where its lifecycle is managed by the subscription manager,
// and it has its own context with its child contexts used for sqs consumption and aborting of the consumption.
// it is also responsible for managing the lifecycle of the subscription handlers.
func (sm *SubscriptionManager) queueConsumerController(queueConsumerCbk func(context.Context)) {
	ctx := context.Background()

	for {
		select {
		case changeEvent := <-sm.topicsChangeCh:
			topic := changeEvent.handler.topic
			sm.logger.Debugf("Subscription change event received with action: %v, on topic: %s", changeEvent.action, topic)
			// topic change events are serialized so that no interleaving can occur
			sm.lock.Lock()
			// although we have a lock here, the topicsHandlers map is thread safe and can be accessed concurrently so other subscribers that are already consuming messages
			// can get the handler for the topic while we're still updating the map without blocking them
			current := sm.topicsHandlers.Size()

			switch changeEvent.action {
			case Subscribe:
				sm.topicsHandlers.Store(topic, changeEvent.handler)
				// if before we've added the subscription there were no subscriptions, this subscribe signals us to start consuming from sqs
				if current == 0 {
					var subCtx context.Context
					// create a new context for sqs consumption with a cancel func to be used when we unsubscribe from all topics
					subCtx, sm.consumeCancelFunc = context.WithCancel(ctx)
					// start sqs consumption
					sm.logger.Info("Starting SQS consumption")
					go queueConsumerCbk(subCtx)
				}
			case Unsubscribe:
				sm.topicsHandlers.Delete(topic)
				// for idempotency, we check the size of the map after the delete operation, as we might have already deleted the subscription
				afterDelete := sm.topicsHandlers.Size()
				// if before we've removed this subscription we had one (last) subscription, this signals us to stop sqs consumption
				if current == 1 && afterDelete == 0 {
					sm.logger.Info("Last subscription removed. no more handlers are mapped to topics. stopping SQS consumption")
					sm.consumeCancelFunc()
				}
			}

			sm.lock.Unlock()
		case <-sm.closeCh:
			return
		}
	}
}

func (sm *SubscriptionManager) Subscribe(topicHandler *SubscriptionTopicHandler) {
	sm.logger.Debug("Subscribing to topic: ", topicHandler.topic)

	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		sm.createSubscribeListener(topicHandler)
	}()
}

func (sm *SubscriptionManager) createSubscribeListener(topicHandler *SubscriptionTopicHandler) {
	sm.logger.Debug("Creating a subscribe listener for topic: ", topicHandler.topic)

	sm.topicsChangeCh <- changeSubscriptionTopicHandler{Subscribe, topicHandler}
	closeCh := make(chan struct{})
	// the unsubscriber is expected to be terminated by the dapr runtime as it cancels the context upon unsubscribe
	go sm.createUnsubscribeListener(topicHandler.ctx, topicHandler.topic, closeCh)
	// if the SubscriptinoManager is being closed and somehow the dapr runtime did not call unsubscribe, we close the control
	// channel here to terminate the unsubscriber and return
	defer close(closeCh)
	<-sm.closeCh
}

// ctx is a context provided by daprd per subscription. unrelated to the consuming sm.baseCtx
func (sm *SubscriptionManager) createUnsubscribeListener(ctx context.Context, topic string, closeCh <-chan struct{}) {
	sm.logger.Debug("Creating an unsubscribe listener for topic: ", topic)

	defer sm.unsubscribe(topic)
	for {
		select {
		case <-ctx.Done():
			return
		case <-closeCh:
			return
		}
	}
}

func (sm *SubscriptionManager) unsubscribe(topic string) {
	sm.logger.Debug("Unsubscribing from topic: ", topic)

	if value, ok := sm.GetSubscriptionTopicHandler(topic); ok {
		sm.topicsChangeCh <- changeSubscriptionTopicHandler{Unsubscribe, value}
	}
}

func (sm *SubscriptionManager) Close() {
	close(sm.closeCh)
	sm.wg.Wait()
}

func (sm *SubscriptionManager) GetSubscriptionTopicHandler(topic string) (*SubscriptionTopicHandler, bool) {
	return sm.topicsHandlers.Load(topic)
}
