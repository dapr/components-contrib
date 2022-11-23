package kubemq

import (
	"context"
	"fmt"
	"strings"
	"time"

	qs "github.com/kubemq-io/kubemq-go/queues_stream"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// interface used to allow unit testing.
type Kubemq interface {
	bindings.InputBinding
	bindings.OutputBinding
}

type kubeMQ struct {
	client    *qs.QueuesStreamClient
	opts      *options
	logger    logger.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc
}

func NewKubeMQ(logger logger.Logger) Kubemq {
	return &kubeMQ{
		client:    nil,
		opts:      nil,
		logger:    logger,
		ctx:       nil,
		ctxCancel: nil,
	}
}

func (k *kubeMQ) Init(metadata bindings.Metadata) error {
	opts, err := createOptions(metadata)
	if err != nil {
		return err
	}
	k.opts = opts
	k.ctx, k.ctxCancel = context.WithCancel(context.Background())
	client, err := qs.NewQueuesStreamClient(k.ctx,
		qs.WithAddress(opts.host, opts.port),
		qs.WithCheckConnection(true),
		qs.WithAuthToken(opts.authToken),
		qs.WithAutoReconnect(true),
		qs.WithReconnectInterval(time.Second))
	if err != nil {
		k.logger.Errorf("error init kubemq client error: %s", err.Error())
		return err
	}
	k.ctx, k.ctxCancel = context.WithCancel(context.Background())
	k.client = client
	return nil
}

func (k *kubeMQ) Read(ctx context.Context, handler bindings.Handler) error {
	go func() {
		for {
			err := k.processQueueMessage(k.ctx, handler)
			if err != nil {
				k.logger.Error(err.Error())
				time.Sleep(time.Second)
			}
			if k.ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

func (k *kubeMQ) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	queueMessage := qs.NewQueueMessage().
		SetChannel(k.opts.channel).
		SetBody(req.Data).
		SetPolicyDelaySeconds(parsePolicyDelaySeconds(req.Metadata)).
		SetPolicyExpirationSeconds(parsePolicyExpirationSeconds(req.Metadata)).
		SetPolicyMaxReceiveCount(parseSetPolicyMaxReceiveCount(req.Metadata)).
		SetPolicyMaxReceiveQueue(parsePolicyMaxReceiveQueue(req.Metadata))
	result, err := k.client.Send(k.ctx, queueMessage)
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

func (k *kubeMQ) processQueueMessage(ctx context.Context, handler bindings.Handler) error {
	pr := qs.NewPollRequest().
		SetChannel(k.opts.channel).
		SetMaxItems(k.opts.pollMaxItems).
		SetWaitTimeout(k.opts.pollTimeoutSeconds).
		SetAutoAck(k.opts.autoAcknowledged)

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
