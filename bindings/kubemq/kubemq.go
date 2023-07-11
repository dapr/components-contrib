package kubemq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	qs "github.com/kubemq-io/kubemq-go/queues_stream"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// interface used to allow unit testing.
type Kubemq interface {
	bindings.InputBinding
	bindings.OutputBinding
}

type kubeMQ struct {
	client  *qs.QueuesStreamClient
	opts    *options
	logger  logger.Logger
	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

func NewKubeMQ(logger logger.Logger) Kubemq {
	return &kubeMQ{
		client:  nil,
		opts:    nil,
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

func (k *kubeMQ) Init(ctx context.Context, metadata bindings.Metadata) error {
	opts, err := createOptions(metadata)
	if err != nil {
		return err
	}
	k.opts = opts
	client, err := qs.NewQueuesStreamClient(ctx,
		qs.WithAddress(opts.internalHost, opts.internalPort),
		qs.WithCheckConnection(true),
		qs.WithAuthToken(opts.AuthToken),
		qs.WithAutoReconnect(true),
		qs.WithReconnectInterval(time.Second))
	if err != nil {
		k.logger.Errorf("error init kubemq client error: %s", err.Error())
		return err
	}
	k.client = client
	return nil
}

func (k *kubeMQ) Read(ctx context.Context, handler bindings.Handler) error {
	if k.closed.Load() {
		return errors.New("binding is closed")
	}
	k.wg.Add(2)
	processCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer k.wg.Done()
		defer cancel()
		select {
		case <-k.closeCh:
		case <-processCtx.Done():
		}
	}()
	go func() {
		defer k.wg.Done()
		for {
			err := k.processQueueMessage(processCtx, handler)
			if err != nil {
				k.logger.Error(err.Error())
			}
			// If context cancelled or kubeMQ closed, exit. Otherwise, continue
			// after a second.
			select {
			case <-time.After(time.Second):
				continue
			case <-processCtx.Done():
			}
			return
		}
	}()
	return nil
}

func (k *kubeMQ) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	queueMessage := qs.NewQueueMessage().
		SetChannel(k.opts.Channel).
		SetBody(req.Data).
		SetPolicyDelaySeconds(parsePolicyDelaySeconds(req.Metadata)).
		SetPolicyExpirationSeconds(parsePolicyExpirationSeconds(req.Metadata)).
		SetPolicyMaxReceiveCount(parseSetPolicyMaxReceiveCount(req.Metadata)).
		SetPolicyMaxReceiveQueue(parsePolicyMaxReceiveQueue(req.Metadata))
	result, err := k.client.Send(ctx, queueMessage)
	if err != nil {
		return nil, err
	}
	if len(result.Results) > 0 {
		if result.Results[0].IsError {
			return nil, fmt.Errorf("error sending queue message: %s", result.Results[0].Error)
		}
	}
	return &bindings.InvokeResponse{
		Data:     nil,
		Metadata: nil,
	}, nil
}

func (k *kubeMQ) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (k *kubeMQ) Close() error {
	if k.closed.CompareAndSwap(false, true) {
		close(k.closeCh)
	}
	defer k.wg.Wait()
	return k.client.Close()
}

func (k *kubeMQ) processQueueMessage(ctx context.Context, handler bindings.Handler) error {
	pr := qs.NewPollRequest().
		SetChannel(k.opts.Channel).
		SetMaxItems(k.opts.PollMaxItems).
		SetWaitTimeout(k.opts.PollTimeoutSeconds).
		SetAutoAck(k.opts.AutoAcknowledged)

	pollResp, err := k.client.Poll(ctx, pr)
	if err != nil {
		if strings.Contains(err.Error(), "timout waiting response") {
			return nil
		}
		return err
	}
	if !pollResp.HasMessages() {
		return nil
	}

	for _, message := range pollResp.Messages {
		_, err := handler(ctx, &bindings.ReadResponse{
			Data: message.Body,
		})
		if err != nil {
			k.logger.Errorf("error received from response handler: %s", err.Error())
			err := message.NAck()
			if err != nil {
				k.logger.Errorf("error processing nack message error: %s", err.Error())
			}
			time.Sleep(time.Second)
			continue
		} else {
			err := message.Ack()
			if err != nil {
				k.logger.Errorf("error processing ack queue message error: %s", err.Error())
				continue
			}
		}
	}
	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (k *kubeMQ) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := options{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
