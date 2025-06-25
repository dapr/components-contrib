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

package utils

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"

	"github.com/gorilla/mux"

	"github.com/dapr/kit/logger"
)

type CommonConfig struct {
	ComponentType string
	ComponentName string
	Operations    map[string]struct{}
}

type server struct {
	Data []byte
}

//nolint:gochecknoglobals
var (
	s          server
	testLogger = logger.NewLogger("utils")
)

// loadEnvFile loads environment variables from a .env file and sets them in the current environment
// This is a simple implementation that overrides existing environment variables
func loadEnvFile(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse key=value pairs
		if parts := strings.SplitN(line, "=", 2); len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			// Remove surrounding quotes if present
			if len(value) >= 2 {
				if (strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)) ||
					(strings.HasPrefix(value, `'`) && strings.HasSuffix(value, `'`)) {
					value = value[1 : len(value)-1]
				}
			}

			// Set the environment variable (overriding existing value)
			os.Setenv(key, value)
		}
	}

	return scanner.Err()
}

// LoadEnvVars loads environment variables from a .env file and overrides existing ones
// This will override any existing environment variables with values from the .env file
func LoadEnvVars(relativePath string) error {
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		return errors.New("cannot get path to current file")
	}

	filepath := path.Join(path.Dir(filename), relativePath)

	// Load .env file and override existing environment variables
	err := loadEnvFile(filepath)
	if err != nil {
		log.Printf("Warning: Error loading .env file from %s: %v", filepath, err)
		log.Printf("Continuing with existing environment variables...")
		return err
	}

	return nil
}

func (cc CommonConfig) HasOperation(operation string) bool {
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
	case http.MethodPost:
		s.handlePost(r)
	case http.MethodGet:
		w.Write(s.handleGet())
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *server) handleGet() []byte {
	return s.Data
}

func (s *server) handlePost(r *http.Request) {
	data, err := io.ReadAll(r.Body)
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
