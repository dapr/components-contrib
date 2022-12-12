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

package cfqueues

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/mitchellh/mapstructure"
	"k8s.io/utils/strings/slices"

	"github.com/dapr/components-contrib/bindings"
	cfworkercode "github.com/dapr/components-contrib/internal/component/cloudflare/worker-src/dist"
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
	// Link to the documentation for the component
	// TODO: Add link to docs
	componentDocsUrl = "https://TODO"
)

// CFQueues is a binding for publishing messages on Cloudflare Queues
type CFQueues struct {
	metadata componentMetadata
	client   *http.Client
	logger   logger.Logger
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewCFQueues returns a new CFQueues.
func NewCFQueues(logger logger.Logger) bindings.OutputBinding {
	return &CFQueues{logger: logger}
}

// Init the component.
func (q *CFQueues) Init(metadata bindings.Metadata) error {
	err := mapstructure.Decode(metadata.Properties, &q.metadata)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}
	err = q.metadata.Validate()
	if err != nil {
		return fmt.Errorf("metadata is invalid: %w", err)
	}

	q.ctx, q.cancel = context.WithCancel(context.Background())

	q.client = &http.Client{
		Timeout: time.Second * 30,
	}

	// Check if we're using an externally-managed worker
	if q.metadata.WorkerURL != "" {
		q.logger.Info("Using externally-managed worker: " + q.metadata.WorkerURL)
		err = q.checkWorker(q.metadata.WorkerURL)
		if err != nil {
			q.logger.Errorf("The component could not be initialized because the externally-managed worker cannot be used: %v", err)
			return err
		}
	} else {
		// We are using a Dapr-managed worker, so let's check if it exists or needs to be created or updated
		err = q.setupWorker()
		if err != nil {
			q.logger.Errorf("The component could not be initialized because the worker cannot be created or updated: %v", err)
			return err
		}
	}

	return nil
}

// Creates or upgrades the worker that is managed by Dapr.
func (q *CFQueues) setupWorker() error {
	// First, get the subdomain for the worker
	// This also acts as a check for the API key
	subdomain, err := q.getWorkersSubdomain()
	if err != nil {
		return fmt.Errorf("error retrieving the workers subdomain from the Cloudflare APIs: %w", err)
	}

	// Check if the worker exists and it's the supported version
	// In case of error, any error, we will re-deploy the worker
	workerUrl := fmt.Sprintf("https://%s.%s.workers.dev/", q.metadata.WorkerName, subdomain)
	err = q.checkWorker(workerUrl)
	if err != nil {
		q.logger.Infof("Deploying updated worker at URL '%s'", workerUrl)
		err = q.deployWorker()
		if err != nil {
			return fmt.Errorf("error deploying or updating the worker with the Cloudflare APIs: %w", err)
		}

		// Ensure the workers.dev route is enabled for the worker
		err = q.enableWorkersDevRoute()
		if err != nil {
			return fmt.Errorf("error enabling the workers.dev route for the worker: %w", err)
		}
		q.logger.Infof("Deployed a new version of the worker at '%s'; note: it may take up to 30s for changes to propagate", workerUrl)
	} else {
		q.logger.Infof("Using worker at URL '%s'", workerUrl)
	}

	// Update the URL of the worker
	q.metadata.WorkerURL = workerUrl

	return nil
}

type cfGetWorkersSubdomainResponse struct {
	Result struct {
		Subdomain string `json:"subdomain"`
	}
}

func (q *CFQueues) getWorkersSubdomain() (string, error) {
	ctx, cancel := context.WithTimeout(q.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.cloudflare.com/client/v4/accounts/"+q.metadata.CfAccountID+"/workers/subdomain", nil)
	if err != nil {
		return "", fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+q.metadata.CfAPIToken)
	req.Header.Set("Content-Type", "application/json")

	res, err := q.client.Do(req)
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
	Main              string                         `json:"main_module"`
	CompatibilityDate string                         `json:"compatibility_date"`
	UsageModel        string                         `json:"usage_model"`
	Bindings          []deployWorkerMetadata_Binding `json:"bindings,omitempty"`
}

type deployWorkerMetadata_Binding struct {
	Name      string  `json:"name"`
	Type      string  `json:"type"`
	Text      *string `json:"text,omitempty"`
	QueueName *string `json:"queue_name,omitempty"`
}

func (q *CFQueues) deployWorker() error {
	// Get the public part of the key as PEM-encoded
	pubKey := q.metadata.privKey.Public()
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
	_, err = field.Write(cfworkercode.WorkerScript)
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
	metadata := deployWorkerMetadata{
		Main:              "worker.js",
		CompatibilityDate: cfworkercode.CompatibilityDate,
		UsageModel:        cfworkercode.UsageModel,
		Bindings: []deployWorkerMetadata_Binding{
			// Variables
			{Type: "plain_text", Name: "PUBLIC_KEY", Text: ptr.Of(string(publicKeyPem))},
			{Type: "plain_text", Name: "TOKEN_AUDIENCE", Text: &q.metadata.WorkerName},
			// Queues
			{Type: "queue", Name: q.metadata.QueueName, QueueName: &q.metadata.QueueName},
		},
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
	ctx, cancel := context.WithTimeout(q.ctx, 30*time.Second)
	defer cancel()

	u := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/workers/scripts/%s", q.metadata.CfAccountID, q.metadata.WorkerName)
	req, err := http.NewRequestWithContext(ctx, "PUT", u, buf)
	if err != nil {
		return fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+q.metadata.CfAPIToken)
	req.Header.Set("Content-Type", mpw.FormDataContentType())

	res, err := q.client.Do(req)
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

func (q *CFQueues) enableWorkersDevRoute() error {
	ctx, cancel := context.WithTimeout(q.ctx, 30*time.Second)
	defer cancel()

	u := fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/workers/scripts/%s/subdomain", q.metadata.CfAccountID, q.metadata.WorkerName)
	req, err := http.NewRequestWithContext(ctx, "POST", u, strings.NewReader(`{"enabled": true}`))
	if err != nil {
		return fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+q.metadata.CfAPIToken)
	req.Header.Set("Content-Type", "application/json")

	res, err := q.client.Do(req)
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

type infoEndpointResponse struct {
	Version string   `json:"version"`
	Queues  []string `json:"queues"`
}

// Check a worker to ensure it's available and it's using a supported version.
func (q *CFQueues) checkWorker(workerURL string) error {
	token, err := q.createToken()
	if err != nil {
		return fmt.Errorf("failed to create authorization token: %w", err)
	}

	ctx, cancel := context.WithTimeout(q.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", workerURL+".well-known/dapr/info", nil)
	if err != nil {
		return fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	res, err := q.client.Do(req)
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

	var data infoEndpointResponse
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		return fmt.Errorf("invalid response from the worker: %w", err)
	}

	version, _ := strconv.Atoi(data.Version)
	if version < minWorkerVersion {
		return fmt.Errorf("the worker is running an outdated version '%d'; please upgrade the worker per instructions in the documentation at %s", version, componentDocsUrl)
	}

	if !slices.Contains(data.Queues, q.metadata.QueueName) {
		return fmt.Errorf("the worker is not bound to the queue '%s'; please re-deploy the worker with the correct bindings per instructions in the documentation at %s", q.metadata.QueueName, componentDocsUrl)
	}

	return nil
}

// Operations returns the supported operations for this binding.
func (q CFQueues) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, "publish"}
}

// Invoke the output binding.
func (q *CFQueues) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation, "publish":
		return q.invokePublish(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", req.Operation)
	}
}

// Handler for invoke operations for publishing messages to the Workers Queue
func (q *CFQueues) invokePublish(parentCtx context.Context, ir *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	token, err := q.createToken()
	if err != nil {
		return nil, fmt.Errorf("failed to create authorization token: %w", err)
	}

	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()

	d, err := strconv.Unquote(string(ir.Data))
	if err == nil {
		ir.Data = []byte(d)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", q.metadata.WorkerURL+"publish/"+q.metadata.QueueName, bytes.NewReader(ir.Data))
	if err != nil {
		return nil, fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	res, err := q.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error invoking the worker: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		res.Body.Close()
	}()
	if res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}

	return nil, nil
}

// Create a JWT token for authorizing requests
func (q CFQueues) createToken() (string, error) {
	now := time.Now()
	token, err := jwt.NewBuilder().
		Audience([]string{q.metadata.WorkerName}).
		Issuer(tokenIssuer).
		IssuedAt(now).
		Expiration(now.Add(tokenExpiration)).
		Build()
	if err != nil {
		return "", fmt.Errorf("failed to build token: %w", err)
	}

	signed, err := jwt.Sign(token, jwt.WithKey(jwa.EdDSA, q.metadata.privKey))
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return string(signed), nil
}

// Close the component
func (q *CFQueues) Close() error {
	if q.cancel != nil {
		q.cancel()
		q.cancel = nil
	}
	return nil
}

// Component metadata struct.
// The component can be initialized in two ways:
// - Instantiate the component with a "workerURL": assumes a worker that has been pre-deployed and it's ready to be used; we will not need API tokens
// - Instantiate the component with a "cfAPIToken" and "cfAccountID": Dapr will take care of creating the worker if it doesn't exist (or upgrade it if needed)
type componentMetadata struct {
	QueueName   string `mapstructure:"queueName"`
	WorkerURL   string `mapstructure:"workerUrl"`
	CfAPIToken  string `mapstructure:"cfAPIToken"`
	CfAccountID string `mapstructure:"cfAccountID"`
	Key         string `mapstructure:"key"`
	WorkerName  string `mapstructure:"workerName"`

	privKey ed25519.PrivateKey
}

var queueNameValidation = regexp.MustCompile("^([a-zA-Z0-9_\\-\\.]+)$")

// Validate the metadata object.
func (m *componentMetadata) Validate() error {
	// Option 1: check if we have a workerURL
	if m.WorkerURL != "" {
		u, err := url.Parse(m.WorkerURL)
		if err != nil {
			return fmt.Errorf("invalid property 'workerUrl': %w", err)
		}
		if u.Scheme != "https" && u.Scheme != "http" {
			return errors.New("invalid property 'workerUrl': unsupported scheme")
		}
		// Re-set the URL to make sure it's sanitized
		m.WorkerURL = u.String()
		if !strings.HasSuffix(m.WorkerURL, "/") {
			m.WorkerURL += "/"
		}
	} else if m.CfAPIToken == "" || m.CfAccountID == "" {
		// Option 2: we need cfAPIToken and cfAccountID
		return errors.New("invalid component metadata: either 'workerUrl' or the combination of 'cfAPIToken'/'cfAccountID' is required")
	}

	// QueueName
	if m.QueueName == "" {
		return errors.New("property 'queueName' is required")
	}
	if !queueNameValidation.MatchString(m.QueueName) {
		return errors.New("metadata property 'queueName' is invalid")
	}

	// WorkerName
	if m.WorkerName == "" {
		return errors.New("property 'workerName' is required")
	}

	// Key
	if m.Key == "" {
		return errors.New("property 'key' is required")
	}
	block, _ := pem.Decode([]byte(m.Key))
	if block == nil || len(block.Bytes) == 0 {
		return errors.New("invalid property 'key': not a PEM-encoded key")
	}
	if block.Type != "PRIVATE KEY" {
		return errors.New("invalid property 'key': not a private key in PKCS#8 format")
	}
	pkAny, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("invalid property 'key': failed to import private key: %w", err)
	}
	pk, ok := pkAny.(ed25519.PrivateKey)
	if !ok {
		return errors.New("invalid property 'key': not an Ed25519 private key")
	}
	m.privKey = pk

	return nil
}
