package snssqs

import (
	"context"
	"sync"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/puzpuzpuz/xsync/v2"
)

type (
	SubscriptionAction int
)

const (
	Subscribe SubscriptionAction = iota
	Unsubscribe
)

var (
	subscriptionMgmtInst *SubscriptionManager
	once                 sync.Once
	initOnce             sync.Once
)

type SubscriptionTopicHandler struct {
	topicName string
	handler   pubsub.Handler
	ctx       context.Context
}

type changeSubscriptionTopicHandler struct {
	action       SubscriptionAction
	topic        string
	topicHandler *SubscriptionTopicHandler
}

type SubscriptionManager struct {
	logger            logger.Logger
	baseContext       context.Context
	consumeCancelFunc context.CancelFunc
	closeCh           chan struct{}
	topicsChangeCh    chan changeSubscriptionTopicHandler
	topicsHandlers    *xsync.MapOf[string, *SubscriptionTopicHandler]
	lock              sync.Mutex
	wg                sync.WaitGroup
}

type SubscriptionManagement interface {
	Init(queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo, cbk func(context.Context, *sqsQueueInfo, *sqsQueueInfo))
	Subscribe(topic string, topicHandler *SubscriptionTopicHandler)
	Close()
	GetSubscriptionTopicHandler(string) (*SubscriptionTopicHandler, bool)
}

func NewSubscriptionMgmt(log logger.Logger) *SubscriptionManager {
	once.Do(func() {
		if subscriptionMgmtInst != nil {
			return
		}

		ctx := context.Background()
		// assign or reassign the singleton
		subscriptionMgmtInst = &SubscriptionManager{
			logger:         log,
			baseContext:    ctx,
			closeCh:        make(chan struct{}),
			topicsChangeCh: make(chan changeSubscriptionTopicHandler),
			topicsHandlers: xsync.NewMapOf[*SubscriptionTopicHandler](),
		}
	})

	return subscriptionMgmtInst
}

func createQueueConsumerCbk(queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo, cbk func(ctx context.Context, queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo)) func(ctx context.Context) {
	return func(ctx context.Context) {
		cbk(ctx, queueInfo, dlqInfo)
	}
}

func (sm *SubscriptionManager) Init(queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo, cbk func(context.Context, *sqsQueueInfo, *sqsQueueInfo)) {
	initOnce.Do(func() {
		sm.logger.Debug("initializing subscription manager")
		queueConsumerCbk := createQueueConsumerCbk(queueInfo, dlqInfo, cbk)
		go subscriptionMgmtInst.queueConsumerController(queueConsumerCbk)
		sm.logger.Debug("subscription manager initialized")
	})
}

func (sm *SubscriptionManager) queueConsumerController(queueConsumerBck func(context.Context)) {
	for {
		select {
		case changeEvent := <-sm.topicsChangeCh:
			sm.logger.Debug("subscription change event", changeEvent.action, changeEvent.topic)
			sm.lock.Lock()
			current := sm.topicsHandlers.Size()
			// unsubscribe:
			// the dapr runtime will cancel the subscriptions' context one by one, as the app is shutting down.
			// we want to stop the sqs consumption when there are no more topics mapped to handlers.
			// this state is reached when the last subscription is cancelled.
			// there is a chance that the SNS to SQS subscription will still send a message to the queue after the last subscription handler is cancelled,
			// as the component is not aware that it is being shutdown and it cannot wait for all the infrastructure subscriptions to be gone
			// (because it doesn't know if a shutdown or just unsubscribe is being performed).
			// if we're in a scenario where topics A,B,C are consumed from queue Q and the component is shutting down,
			// there is a possible race condition between the infrastructure and dapr, where the component will stop consuming from Q after the last subscription is cancelled,
			// while messages from topic C are still being sent to Q. If a boot with only subscriptions to topics A,B will follow the shutdown,
			// and messages from topic C where not drained from Q, they will be consumed by the component but there will be no handler for them,
			// thereby causing errors in the component indicating that there is no handler for topic C.
			if changeEvent.action == Unsubscribe {
				sm.logger.Debug("unsubscribing from topic", changeEvent.topic)
				sm.topicsHandlers.Delete(changeEvent.topic)
				// if before we've removed this subscription we had one (last) subscription, this signals us to stop sqs consumption
				if current == 1 {
					sm.logger.Info("last subscription removed, stopping sqs consumption")
					sm.consumeCancelFunc()
				}
				// subscribe
			} else if changeEvent.action == Subscribe {
				sm.logger.Debug("subscribing to topic", changeEvent.topic)
				sm.topicsHandlers.Store(changeEvent.topic, changeEvent.topicHandler)
				// if before we've added the subscription there were no subscription, this subscribe signals us to start consume from sqs
				if current == 0 {
					subctx, cancel := context.WithCancel(sm.baseContext)
					sm.consumeCancelFunc = cancel
					sm.logger.Debug("starting sqs consumption for the first time")
					go queueConsumerBck(subctx)
				}
			}
			sm.lock.Unlock()
		case <-sm.closeCh:
			return
		}
	}
}

func (sm *SubscriptionManager) Subscribe(topic string, topicHandler *SubscriptionTopicHandler) {
	sm.logger.Debug("subscribing to topic", topic)
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		sm.createSubscribeListener(topic, topicHandler)
	}()
}

func (sm *SubscriptionManager) createSubscribeListener(topic string, topicHandler *SubscriptionTopicHandler) {
	sm.logger.Debug("creating subscribe listener for topic ", topic)
	sm.topicsChangeCh <- changeSubscriptionTopicHandler{Subscribe, topic, topicHandler}
	sm.logger.Debug("calling creating unsubscribe listener for topic ", topic)
	closeCh := make(chan struct{})
	go sm.createUnsubscribeListener(topicHandler.ctx, topic, closeCh)

	for range sm.closeCh {
		close(closeCh)

		return
	}

}

// ctx is a context provided by daprd per subscription. unrelated to the consuming sm.baseCtx
func (sm *SubscriptionManager) createUnsubscribeListener(ctx context.Context, topic string, closeCh <-chan struct{}) {
	sm.logger.Debug("creating unsubscribe listener for topic ", topic)
	for {
		select {
		case <-ctx.Done():
			if value, ok := sm.GetSubscriptionTopicHandler(topic); ok {
				sm.topicsChangeCh <- changeSubscriptionTopicHandler{Unsubscribe, topic, value}
			}
			// no need to iterate anymore as this subctx is exhusted
			return
		case <-closeCh:
			return
		}
	}
}

func (sm *SubscriptionManager) Close() {
	close(sm.closeCh)
	sm.wg.Wait()
}

func (sm *SubscriptionManager) GetSubscriptionTopicHandler(topic string) (*SubscriptionTopicHandler, bool) {
	return sm.topicsHandlers.Load(topic)
}
