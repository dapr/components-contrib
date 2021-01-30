// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package utils

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/dapr/dapr/pkg/logger"
	"github.com/gorilla/mux"
	"k8s.io/apimachinery/pkg/util/sets"
)

type CommonConfig struct {
	ComponentType string
	ComponentName string
	AllOperations bool
	Operations    sets.String
}

type server struct {
	Data []byte
}

// nolint:gochecknoglobals
var (
	s          server
	testLogger = logger.NewLogger("utils")
)

func (cc CommonConfig) HasOperation(operation string) bool {
	return cc.AllOperations || cc.Operations.Has(operation)
}

func (cc CommonConfig) CopyMap(config map[string]string) map[string]string {
	m := map[string]string{}
	for k, v := range config {
		m[k] = v
	}

	return m
}

func StartHTTPServer(port int, ready chan bool) {
	s = server{}
	srv := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: appRouter(),
	}

	testLogger.Info(("Starting HTTP Server"))
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			testLogger.Errorf("Error with http server: %s", err.Error())
		}
	}()

	testLogger.Info(("Registering Signal"))
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	ready <- true
	testLogger.Info(("Waiting to stop Server"))
	<-stop
	testLogger.Info(("Stopping Server"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		testLogger.Errorf("Error shutting down http server: %s", err.Error())
	}
}

func appRouter() *mux.Router {
	router := mux.NewRouter().StrictSlash(true)

	router.HandleFunc("/call", handleCall).Methods("POST")

	router.Use(mux.CORSMethodMiddleware(router))

	return router
}

func handleCall(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		s.handlePost(r)
	case "GET":
		w.Write(s.handleGet())
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *server) handleGet() []byte {
	return s.Data
}

func (s *server) handlePost(r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	if err == nil {
		s.Data = data
	}
}
