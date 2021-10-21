// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package app

import (
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
		return s.Stop()
	}

	return nil
}
