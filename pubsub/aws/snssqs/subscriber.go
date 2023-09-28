package snssqs

import (
	"context"
	"sync"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/puzpuzpuz/xsync/v2"
)

var subscriptionMgmtInst *SubscriptionMgmt
var once sync.Once

type subscriptionTopicHandler struct {
	topicName string
	handler   pubsub.Handler
	ctx       context.Context
}

type changeSubscriptionTopicHandler struct {
	op    int
	topic string
	sth   *subscriptionTopicHandler
}

type SubscriptionMgmt struct {
	baseContext       context.Context
	consumeCancelFunc context.CancelFunc
	queueInfo         *sqsQueueInfo
	dlqInfo           *sqsQueueInfo
	cbk               func(context.Context, *sqsQueueInfo, *sqsQueueInfo)
	closeCh           chan interface{}
	topicsChangeCh    chan changeSubscriptionTopicHandler
	topicsHandlers    *xsync.MapOf[string, *subscriptionTopicHandler]
	lock              sync.Mutex
}

type SubscriptionMgmtCtrl interface {
	Subscribe()
	Close()
	Get(string) (*subscriptionTopicHandler, bool)
}

func NewSubscriptionMgmt(queueInfo *sqsQueueInfo, dlqInfo *sqsQueueInfo) *SubscriptionMgmt {
	once.Do(func() {
		if subscriptionMgmtInst != nil {
			return
		}

		ctx := context.Background()
		sm := &SubscriptionMgmt{baseContext: ctx, queueInfo: queueInfo, dlqInfo: dlqInfo, topicsHandlers: xsync.NewMapOf[*subscriptionTopicHandler]()}
		subscriptionMgmtInst = sm
		go subscriptionMgmtInst.consumerController()
	})

	return subscriptionMgmtInst
}

func (sm *SubscriptionMgmt) consumerController() {
	for {
		select {
		case changeEvent := <-sm.topicsChangeCh:
			sm.lock.Lock()
			current := sm.topicsHandlers.Size()
			// unsubscribe
			if changeEvent.op == -1 {
				sm.topicsHandlers.Delete(changeEvent.topic)
				// if before we've removed this subscription we had one (last) subscription, this signals us to stop sqs consumption
				if current == 1 {
					sm.consumeCancelFunc()
				}
				// subscribe
			} else if changeEvent.op == 1 {
				sm.topicsHandlers.Store(changeEvent.topic, changeEvent.sth)
				// if before we've added the subscription there were no subscription, this subscribe signals us to start consume from sqs
				if current == 0 {
					subctx, cancel := context.WithCancel(sm.baseContext)
					sm.consumeCancelFunc = cancel
					go sm.cbk(subctx, sm.queueInfo, sm.dlqInfo)
				}
			}
			sm.lock.Unlock()
		case <-sm.closeCh:
			return
		}
	}
}

func (sm *SubscriptionMgmt) Subscribe(topic string, sth *subscriptionTopicHandler) {
	sm.topicsChangeCh <- changeSubscriptionTopicHandler{1, topic, sth}
	sm.AddUnsubscribeListener(sm.closeCh, sth.ctx, topic)

}

// ctx is a context provided by daprd per subscription. unrelated to the consuming sm.baseCtx
func (sm *SubscriptionMgmt) AddUnsubscribeListener(done chan interface{}, ctx context.Context, topic string) {
	go func() {
		// label the subscription context so that upstream (fan-in) channel would be able to remove the correct handler
		subctx := context.WithValue(ctx, "topic", topic)
		for {
			select {
			case <-subctx.Done():
				key := subctx.Value("topic").(string)
				if value, ok := sm.Get(key); ok {
					sm.topicsChangeCh <- changeSubscriptionTopicHandler{-1, key, value}
				}
				// no need to iterate anymore as this subctx is exhusted
				return
			case <-done:
				return
			}
		}
	}()
}

func (sm *SubscriptionMgmt) Close() {
	close(sm.closeCh)
}

func (sm *SubscriptionMgmt) Get(topic string) (*subscriptionTopicHandler, bool) {
	return sm.topicsHandlers.Load(topic)
}
