// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package utils

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"

	"github.com/gorilla/mux"

	"github.com/dapr/kit/logger"
)

type CommonConfig struct {
	ComponentType string
	ComponentName string
	AllOperations bool
	Operations    map[string]struct{}
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
	if cc.AllOperations {
		return true
	}
	_, exists := cc.Operations[operation]

	return exists
}

func (cc CommonConfig) CopyMap(config map[string]string) map[string]string {
	m := map[string]string{}
	for k, v := range config {
		m[k] = v
	}

	return m
}

func StartHTTPServer(port int, ready chan bool) {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		testLogger.Errorf("Error starting test HTTP serer: %v", err)

		return
	}

	testLogger.Info(("Starting HTTP Server"))
	ts := httptest.NewUnstartedServer(appRouter())
	// NewUnstartedServer creates a listener. Close that listener and replace
	// with the one we created.
	ts.Listener.Close()
	ts.Listener = l

	// Start the server.
	ts.Start()
	defer ts.Close()

	ready <- true

	testLogger.Info(("Registering Signal"))
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	testLogger.Info(("Waiting to stop Server"))
	<-stop
	testLogger.Info(("Stopping Server"))
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

func NewStringSet(values ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}

	return set
}
