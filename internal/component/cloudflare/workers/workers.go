/*
Copyright 2022 The Dapr Authors
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

// Package workers contains a base class that is used by components that are based on Cloudflare Workers.
package workers

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/components-contrib/bindings"
	cfworkerscode "github.com/dapr/components-contrib/internal/component/cloudflare/workers/code"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const (
	// Minimum version required for the running Worker.
	minWorkerVersion = 20221209
	// Issuer for JWTs
	tokenIssuer = "dapr.io/cloudflare"
	// JWT token expiration
	tokenExpiration = 5 * time.Minute
)

// Base is a base class for components that rely on Cloudflare Base
type Base struct {
	metadata             *BaseMetadata
	infoResponseValidate func(*InfoEndpointResponse) error
	componentDocsUrl     string
	client               *http.Client
	logger               logger.Logger
	ctx                  context.Context
	cancel               context.CancelFunc
}

// Init the base class.
func (w *Base) Init(metadata bindings.Metadata, workerBindings []Binding, componentDocsUrl string, infoResponseValidate func(*InfoEndpointResponse) error) (err error) {
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.client = &http.Client{
		Timeout: time.Second * 30,
	}
	w.componentDocsUrl = componentDocsUrl
	w.infoResponseValidate = infoResponseValidate

	// Check if we're using an externally-managed worker
	if w.metadata.WorkerURL != "" {
		w.logger.Info("Using externally-managed worker: " + w.metadata.WorkerURL)
		err = w.checkWorker(w.metadata.WorkerURL)
		if err != nil {
			w.logger.Errorf("The component could not be initialized because the externally-managed worker cannot be used: %v", err)
			return err
		}
	} else {
		// We are using a Dapr-managed worker, so let's check if it exists or needs to be created or updated
		err = w.setupWorker(workerBindings)
		if err != nil {
			w.logger.Errorf("The component could not be initialized because the worker cannot be created or updated: %v", err)
			return err
		}
	}

	return nil
}

// SetLogger sets the logger object.
func (w *Base) SetLogger(logger logger.Logger) {
	w.logger = logger
}

// SetMetadata sets the metadata for the base object.
func (w *Base) SetMetadata(metadata *BaseMetadata) {
	w.metadata = metadata
}

// Client returns the HTTP client.
func (w Base) Client() *http.Client {
	return w.client
}

// Creates or upgrades the worker that is managed by Dapr.
func (w *Base) setupWorker(workerBindings []Binding) error {
	// First, get the subdomain for the worker
	// This also acts as a check for the API key
	subdomain, err := w.getWorkersSubdomain()
	if err != nil {
		return fmt.Errorf("error retrieving the workers subdomain from the Cloudflare APIs: %w", err)
	}

	// Check if the worker exists and it's the supported version
	// In case of error, any error, we will re-deploy the worker
	workerUrl := fmt.Sprintf("https://%s.%s.workers.dev/", w.metadata.WorkerName, subdomain)
	err = w.checkWorker(workerUrl)
	if err != nil {
		w.logger.Infof("Deploying updated worker at URL '%s'", workerUrl)
		err = w.deployWorker(workerBindings)
		if err != nil {
			return fmt.Errorf("error deploying or updating the worker with the Cloudflare APIs: %w", err)
		}

		// Ensure the workers.dev route is enabled for the worker
		err = w.enableWorkersDevRoute()
		if err != nil {
			return fmt.Errorf("error enabling the workers.dev route for the worker: %w", err)
		}

		// Wait for the worker to be deplopyed, which can take up to 30 seconds (but let's give it 1 minute)
		w.logger.Debugf("Deployed a new version of the worker at '%s' - waiting for propagation", workerUrl)
		start := time.Now()
		for time.Since(start) < time.Minute {
			err = w.checkWorker(workerUrl)
			if err == nil {
				break
			}
			w.logger.Debug("Worker is not yet ready - trying again in 3s")
			time.Sleep(3 * time.Second)
		}
		if err != nil {
			return fmt.Errorf("worker was not ready in 1 minute; last check failed with error: %w", err)
		}
		w.logger.Debug("Worker is ready")
	} else {
		w.logger.Infof("Using worker at URL '%s'", workerUrl)
	}

	// Update the URL of the worker
	w.metadata.WorkerURL = workerUrl

	return nil
}

type cfGetWorkersSubdomainResponse struct {
	Result struct {
		Subdomain string `json:"subdomain"`
	}
}

func (w *Base) getWorkersSubdomain() (string, error) {
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()

	u := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/workers/subdomain", w.metadata.CfAccountID)
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "", fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+w.metadata.CfAPIToken)
	req.Header.Set("Content-Type", "application/json")

	res, err := w.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error invoking the service: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		res.Body.Close()
	}()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}

	var data cfGetWorkersSubdomainResponse
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		return "", fmt.Errorf("invalid response format: %w", err)
	}

	if data.Result.Subdomain == "" {
		return "", fmt.Errorf("response does not contain a value for 'subdomain'")
	}

	return data.Result.Subdomain, nil
}

type deployWorkerMetadata struct {
	Main              string    `json:"main_module"`
	CompatibilityDate string    `json:"compatibility_date"`
	UsageModel        string    `json:"usage_model"`
	Bindings          []Binding `json:"bindings,omitempty"`
}

// Binding contains a binding that is attached to the worker
type Binding struct {
	Name      string  `json:"name"`
	Type      string  `json:"type"`
	Text      *string `json:"text,omitempty"`
	QueueName *string `json:"queue_name,omitempty"`
}

func (w *Base) deployWorker(workerBindings []Binding) error {
	// Get the public part of the key as PEM-encoded
	pubKey := w.metadata.privKey.Public()
	pubKeyDer, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		return fmt.Errorf("failed to marshal public key to PKIX: %w", err)
	}
	publicKeyPem := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubKeyDer,
	})

	// Request body
	buf := &bytes.Buffer{}
	mpw := multipart.NewWriter(buf)
	mh := textproto.MIMEHeader{}

	// Script module
	mh.Set("Content-Type", "application/javascript+module")
	mh.Set("Content-Disposition", `form-data; name="worker.js"; filename="worker.js"`)
	field, err := mpw.CreatePart(mh)
	if err != nil {
		return fmt.Errorf("failed to create field worker.js: %w", err)
	}
	_, err = field.Write(cfworkerscode.WorkerScript)
	if err != nil {
		return fmt.Errorf("failed to write field worker.js: %w", err)
	}

	// Metadata
	mh.Set("Content-Type", "application/json")
	mh.Set("Content-Disposition", `form-data; name="metadata"; filename="metadata.json"`)
	field, err = mpw.CreateFormField("metadata")
	if err != nil {
		return fmt.Errorf("failed to create field metadata: %w", err)
	}
	// Add variables to bindings
	workerBindings = append(workerBindings,
		Binding{Type: "plain_text", Name: "PUBLIC_KEY", Text: ptr.Of(string(publicKeyPem))},
		Binding{Type: "plain_text", Name: "TOKEN_AUDIENCE", Text: &w.metadata.WorkerName},
	)
	metadata := deployWorkerMetadata{
		Main:              "worker.js",
		CompatibilityDate: cfworkerscode.CompatibilityDate,
		UsageModel:        cfworkerscode.UsageModel,
		Bindings:          workerBindings,
	}
	enc := json.NewEncoder(field)
	enc.SetEscapeHTML(false)
	err = enc.Encode(&metadata)
	if err != nil {
		return fmt.Errorf("failed to encode metadata as JSON: %w", err)
	}

	// Complete the body
	err = mpw.Close()
	if err != nil {
		return fmt.Errorf("failed to close multipart body: %w", err)
	}

	// Make the request
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()

	u := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/workers/scripts/%s", w.metadata.CfAccountID, w.metadata.WorkerName)
	req, err := http.NewRequestWithContext(ctx, "PUT", u, buf)
	if err != nil {
		return fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+w.metadata.CfAPIToken)
	req.Header.Set("Content-Type", mpw.FormDataContentType())

	res, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("error invoking the service: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		res.Body.Close()
	}()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}

	return nil
}

func (w *Base) enableWorkersDevRoute() error {
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()

	u := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/workers/scripts/%s/subdomain", w.metadata.CfAccountID, w.metadata.WorkerName)
	req, err := http.NewRequestWithContext(ctx, "POST", u, strings.NewReader(`{"enabled": true}`))
	if err != nil {
		return fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+w.metadata.CfAPIToken)
	req.Header.Set("Content-Type", "application/json")

	res, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("error invoking the service: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		res.Body.Close()
	}()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}

	return nil
}

// Object containing the response from the info endpoint
type InfoEndpointResponse struct {
	Version string   `json:"version"`
	Queues  []string `json:"queues"`
}

// Check a worker to ensure it's available and it's using a supported version.
func (w *Base) checkWorker(workerURL string) error {
	token, err := w.metadata.CreateToken()
	if err != nil {
		return fmt.Errorf("failed to create authorization token: %w", err)
	}

	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", workerURL+".well-known/dapr/info", nil)
	if err != nil {
		return fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	res, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("error invoking the worker: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		res.Body.Close()
	}()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}

	var data InfoEndpointResponse
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		return fmt.Errorf("invalid response from the worker: %w", err)
	}

	version, _ := strconv.Atoi(data.Version)
	if version < minWorkerVersion {
		return fmt.Errorf("the worker is running an outdated version '%d'; please upgrade the worker per instructions in the documentation at %s", version, w.componentDocsUrl)
	}

	if w.infoResponseValidate != nil {
		err = w.infoResponseValidate(&data)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close the base component.
func (q *Base) Close() error {
	if q.cancel != nil {
		q.cancel()
		q.cancel = nil
	}
	return nil
}
