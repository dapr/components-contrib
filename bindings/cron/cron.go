/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cron

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// Binding represents Cron input binding.
type Binding struct {
	logger   logger.Logger
	name     string
	schedule string
	parser   cron.Parser
}

var (
	_      = bindings.InputBinding(&Binding{})
	stopCh = make(map[string]chan bool)
)

// NewCron returns a new Cron event input binding.
func NewCron(logger logger.Logger) *Binding {
	return &Binding{
		logger: logger,
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
	if _, ok := stopCh[metadata.Name]; !ok {
		stopCh[metadata.Name] = make(chan bool)
	}
	b.name = metadata.Name
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

// Read triggers the Cron scheduler.
func (b *Binding) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	c := cron.New(cron.WithParser(b.parser))
	id, err := c.AddFunc(b.schedule, func() {
		b.logger.Debugf("name: %s, schedule fired: %v", b.name, time.Now())
		handler(&bindings.ReadResponse{
			Metadata: map[string]string{
				"timeZone":    c.Location().String(),
				"readTimeUTC": time.Now().UTC().String(),
			},
		})
	})
	if err != nil {
		return errors.Wrapf(err, "name: %s, error scheduling %s", b.name, b.schedule)
	}
	c.Start()
	b.logger.Debugf("name: %s, next run: %v", b.name, time.Until(c.Entry(id).Next))
	<-stopCh[b.name]
	b.logger.Debugf("name: %s, stopping schedule: %s", b.name, b.schedule)
	c.Stop()

	return nil
}

// Invoke exposes way to stop previously started cron.
func (b *Binding) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	b.logger.Debugf("name: %s, operation: %v", b.name, req.Operation)
	if req.Operation != bindings.DeleteOperation {
		return nil, fmt.Errorf("invalid operation: '%v', only '%v' supported",
			req.Operation, bindings.DeleteOperation)
	}
	stopCh[b.name] <- true

	return &bindings.InvokeResponse{
		Metadata: map[string]string{
			"schedule":    b.schedule,
			"stopTimeUTC": time.Now().UTC().String(),
		},
	}, nil
}

// Operations method returns the supported operations by this binding.
func (b *Binding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.DeleteOperation,
	}
}
