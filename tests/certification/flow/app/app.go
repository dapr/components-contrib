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

package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/dapr/go-sdk/service/common"
	daprd "github.com/dapr/go-sdk/service/http"

	"github.com/dapr/components-contrib/tests/certification/flow"
)

type App struct {
	appName string
	address string
	setup   SetupFn
}

type SetupFn func(flow.Context, common.Service) error

func Run(appName, address string, setup SetupFn) (string, flow.Runnable, flow.Runnable) {
	return New(appName, address, setup).ToStep()
}

func New(appName, address string, setup SetupFn) App {
	return App{
		appName: appName,
		address: address,
		setup:   setup,
	}
}

func (a App) AppName() string {
	return a.appName
}

func (a App) ToStep() (string, flow.Runnable, flow.Runnable) {
	return a.appName, a.Start, a.Stop
}

func Start(appName, address string, setup SetupFn) flow.Runnable {
	return New(appName, address, setup).Start
}

func (a App) Start(ctx flow.Context) error {
	s := daprd.NewService(a.address)

	if err := a.setup(ctx, s); err != nil {
		return err
	}

	go func() {
		if err := s.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("error listenning: %v", err)
		}
	}()

	ctx.Set(a.appName, s)

	return nil
}

func Stop(appName string) flow.Runnable {
	return App{appName: appName}.Stop
}

func (a App) Stop(ctx flow.Context) error {
	var s common.Service
	if ctx.Get(a.appName, &s) {
		err := s.Stop()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// If the error is a context deadline exceeded, it just means the HTTP server couldn't shut down gracefully
				// We will log that error but we can safely ignore it
				log.Printf("Service did not shut down gracefully - ignoring the error: %s", err)
				return nil
			}
			return fmt.Errorf("failed to stop service: %w", err)
		}
	}

	return nil
}
