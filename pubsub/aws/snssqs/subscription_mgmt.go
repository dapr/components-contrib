package snssqs

import (
	"context"
	"sync"

	"github.com/dapr/components-contrib/pubsub"
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
)

type subscriptionTopicHandler struct {
	topicName string
	handler   pubsub.Handler
	ctx       context.Context
}

type changeSubscriptionTopicHandler struct {
	action       SubscriptionAction
	topic        string
	topicHandler *subscriptionTopicHandler
}

type SubscriptionManager struct {
	baseContext       context.Context
	consumeCancelFunc context.CancelFunc
	closeCh           chan struct{}
	topicsChangeCh    chan changeSubscriptionTopicHandler
	topicsHandlers    *xsync.MapOf[string, *subscriptionTopicHandler]
	lock              sync.Mutex
	wg                sync.WaitGroup
}

type SubscriptionManagement interface {
	Subscribe()
	Close()
	GetTopicHandler(string) (*subscriptionTopicHandler, bool)
}

func NewSubscriptionMgmt(queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo, cbk func(context.Context, *sqsQueueInfo, *sqsQueueInfo)) *SubscriptionManager {
	once.Do(func() {
		if subscriptionMgmtInst != nil {
			return
		}

		ctx := context.Background()
		// assign or reassign the singleton
		subscriptionMgmtInst = &SubscriptionManager{
			baseContext:    ctx,
			closeCh:        make(chan struct{}),
			topicsHandlers: xsync.NewMapOf[*subscriptionTopicHandler](),
		}

		go subscriptionMgmtInst.sqsConsumerController(queueInfo, dlqInfo, cbk)
	})

	return subscriptionMgmtInst
}

func (sm *SubscriptionManager) sqsConsumerController(queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo, cbk func(context.Context, *sqsQueueInfo, *sqsQueueInfo)) {
	for {
		select {
		case changeEvent := <-sm.topicsChangeCh:
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
				sm.topicsHandlers.Delete(changeEvent.topic)
				// if before we've removed this subscription we had one (last) subscription, this signals us to stop sqs consumption
				if current == 1 {
					sm.consumeCancelFunc()
				}
				// subscribe
			} else if changeEvent.action == Subscribe {
				sm.topicsHandlers.Store(changeEvent.topic, changeEvent.topicHandler)
				// if before we've added the subscription there were no subscription, this subscribe signals us to start consume from sqs
				if current == 0 {
					subctx, cancel := context.WithCancel(sm.baseContext)
					sm.consumeCancelFunc = cancel
					go cbk(subctx, queueInfo, dlqInfo)
				}
			}
			sm.lock.Unlock()
		case <-sm.closeCh:
			return
		}
	}
}

func (sm *SubscriptionManager) Subscribe(topic string, topicHandler *subscriptionTopicHandler) {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		sm.createSubscribeListener(topic, topicHandler)
	}()
}

func (sm *SubscriptionManager) createSubscribeListener(topic string, topicHandler *subscriptionTopicHandler) {
	sm.topicsChangeCh <- changeSubscriptionTopicHandler{Subscribe, topic, topicHandler}

	closeCh := make(chan struct{})
	go sm.createUnsubscribeListener(topic, topicHandler.ctx, closeCh)

	for range sm.closeCh {
		close(closeCh)

		return
	}

}

// ctx is a context provided by daprd per subscription. unrelated to the consuming sm.baseCtx
func (sm *SubscriptionManager) createUnsubscribeListener(topic string, ctx context.Context, closeCh <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			if value, ok := sm.GetTopicHandler(topic); ok {
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

func (sm *SubscriptionManager) GetTopicHandler(topic string) (*subscriptionTopicHandler, bool) {
	return sm.topicsHandlers.Load(topic)
}
