// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cron

import (
	"fmt"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
)

// Binding represents Cron input binding
type Binding struct {
	logger   logger.Logger
	schedule string
	stopCh   chan bool
	parser   cron.Parser
}

var _ = bindings.InputBinding(&Binding{})

// NewCron returns a new Cron event input binding
func NewCron(logger logger.Logger) *Binding {
	return &Binding{
		logger: logger,
		stopCh: make(chan bool),
		parser: cron.NewParser(
			cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
		),
	}
}

// Init initializes the Cron binding
// Examples from https://godoc.org/github.com/robfig/cron:
//   "15 * * * * *" - Every 15 sec
//   "0 30 * * * *" - Every 30 min
func (b *Binding) Init(metadata bindings.Metadata) error {
	s, f := metadata.Properties["schedule"]
	if !f || s == "" {
		return fmt.Errorf("schedule not set")
	}
	_, err := b.parser.Parse(s)
	if err != nil {
		return errors.Wrapf(err, "invalid schedule format: %s", s)
	}
	b.schedule = s

	return nil
}

// Read triggers the Cron scheduler
func (b *Binding) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	c := cron.New(cron.WithParser(b.parser))
	id, err := c.AddFunc(b.schedule, func() {
		b.logger.Debugf("schedule fired: %v", time.Now())
		handler(&bindings.ReadResponse{
			Metadata: map[string]string{
				"timeZone":    c.Location().String(),
				"readTimeUTC": time.Now().UTC().String(),
			},
		})
	})
	if err != nil {
		return errors.Wrapf(err, "error scheduling %s", b.schedule)
	}
	c.Start()
	b.logger.Debugf("next run: %v", time.Until(c.Entry(id).Next))
	<-b.stopCh
	b.logger.Debugf("stopping schedule: %s", b.schedule)
	c.Stop()

	return nil
}

// Invoke exposes way to stop previously started cron
func (b *Binding) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	b.logger.Debugf("operation: %v", req.Operation)
	if req.Operation != bindings.DeleteOperation {
		return nil, fmt.Errorf("invalid operation: '%v', only '%v' supported",
			req.Operation, bindings.DeleteOperation)
	}
	b.stopCh <- true

	return &bindings.InvokeResponse{
		Metadata: map[string]string{
			"schedule":    b.schedule,
			"stopTimeUTC": time.Now().UTC().String(),
		},
	}, nil
}

// Operations method returns the supported operations by this binding
func (b *Binding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.DeleteOperation,
	}
}
