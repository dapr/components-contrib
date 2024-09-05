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
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/utils/clock"

	"github.com/dapr/components-contrib/bindings"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	cron "github.com/dapr/kit/cron"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// Binding represents Cron input binding.
type Binding struct {
	logger   logger.Logger
	name     string
	schedule string
	parser   cron.Parser
	clk      clock.Clock
	closed   atomic.Bool
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

type metadata struct {
	Schedule string
}

// NewCron returns a new Cron event input binding.
func NewCron(logger logger.Logger) bindings.InputBinding {
	return NewCronWithClock(logger, clock.RealClock{})
}

func NewCronWithClock(logger logger.Logger, clk clock.Clock) bindings.InputBinding {
	return &Binding{
		logger: logger,
		clk:    clk,
		parser: cron.NewParser(
			cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
		),
		closeCh: make(chan struct{}),
	}
}

// Init initializes the Cron binding
// Examples from https://godoc.org/github.com/robfig/cron:
//
//	"15 * * * * *" - Every 15 sec
//	"0 30 * * * *" - Every 30 min
func (b *Binding) Init(ctx context.Context, meta bindings.Metadata) error {
	b.name = meta.Name
	m := metadata{}
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}
	if m.Schedule == "" {
		return errors.New("schedule not set")
	}
	_, err = b.parser.Parse(m.Schedule)
	if err != nil {
		return fmt.Errorf("invalid schedule format '%s': %w", m.Schedule, err)
	}
	b.schedule = m.Schedule

	return nil
}

// Read triggers the Cron scheduler.
func (b *Binding) Read(ctx context.Context, handler bindings.Handler) error {
	if b.closed.Load() {
		return errors.New("binding is closed")
	}

	c := cron.New(cron.WithParser(b.parser), cron.WithClock(b.clk))
	id, err := c.AddFunc(b.schedule, func() {
		b.logger.Debugf("name: %s, schedule fired: %v", b.name, time.Now())
		handler(ctx, &bindings.ReadResponse{
			Metadata: map[string]string{
				"timeZone":    c.Location().String(),
				"readTimeUTC": time.Now().UTC().String(),
			},
		})
	})
	if err != nil {
		return fmt.Errorf("name: %s, error scheduling %s: %w", b.name, b.schedule, err)
	}
	c.Start()
	b.logger.Debugf("name: %s, next run: %v", b.name, time.Until(c.Entry(id).Next))

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		// Wait for context to be canceled or component to be closed.
		select {
		case <-ctx.Done():
		case <-b.closeCh:
		}
		b.logger.Debugf("name: %s, stopping schedule: %s", b.name, b.schedule)
		c.Stop()
	}()

	return nil
}

func (b *Binding) Close() error {
	if b.closed.CompareAndSwap(false, true) {
		close(b.closeCh)
	}
	b.wg.Wait()
	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (b *Binding) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := metadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}
