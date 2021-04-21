// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package jobworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/zeebe-io/zeebe/clients/go/pkg/entities"
	"github.com/zeebe-io/zeebe/clients/go/pkg/worker"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"
)

var (
	ErrMissingJobType = errors.New("jobType is a required attribute")
)

// ZeebeJobWorker allows handling jobs from the Zeebe command engine
type ZeebeJobWorker struct {
	clientFactory zeebe.ClientFactory
	client        zbc.Client
	metadata      *jobWorkerMetadata
	logger        logger.Logger
}

// https://docs.zeebe.io/basics/job-workers.html
type jobWorkerMetadata struct {
	WorkerName     string            `json:"workerName"`
	WorkerTimeout  metadata.Duration `json:"workerTimeout"`
	RequestTimeout metadata.Duration `json:"requestTimeout"`
	JobType        string            `json:"jobType"`
	MaxJobsActive  int               `json:"maxJobsActive,string"`
	Concurrency    int               `json:"concurrency,string"`
	PollInterval   metadata.Duration `json:"pollInterval"`
	PollThreshold  float64           `json:"pollThreshold,string"`
	FetchVariables string            `json:"fetchVariables"`
}

type jobHandler struct {
	callback func(*bindings.ReadResponse) ([]byte, error)
	logger   logger.Logger
}

// NewZeebeJobWorker returns a new ZeebeJobWorker instance
func NewZeebeJobWorker(logger logger.Logger) *ZeebeJobWorker {
	return &ZeebeJobWorker{clientFactory: zeebe.NewClientFactoryImpl(logger), logger: logger}
}

// Init does metadata parsing and connection creation
func (z *ZeebeJobWorker) Init(metadata bindings.Metadata) error {
	meta, err := z.parseMetadata(metadata)
	if err != nil {
		return err
	}

	if meta.JobType == "" {
		return ErrMissingJobType
	}

	client, err := z.clientFactory.Get(metadata)
	if err != nil {
		return err
	}

	z.metadata = meta
	z.client = client

	return nil
}

func (z *ZeebeJobWorker) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	h := jobHandler{
		callback: handler,
		logger:   z.logger,
	}

	jobWorker := z.getJobWorker(h)

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM)
	<-exitChan

	jobWorker.Close()
	jobWorker.AwaitClose()

	if err := z.client.Close(); err != nil {
		return err
	}

	return nil
}

func (z *ZeebeJobWorker) parseMetadata(metadata bindings.Metadata) (*jobWorkerMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m jobWorkerMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (z *ZeebeJobWorker) getJobWorker(handler jobHandler) worker.JobWorker {
	return z.client.
		NewJobWorker().
		JobType(z.metadata.JobType).
		Handler(handler.handleJob).
		Name(z.metadata.WorkerName).
		Timeout(z.metadata.WorkerTimeout.Duration).
		RequestTimeout(z.metadata.RequestTimeout.Duration).
		MaxJobsActive(z.metadata.MaxJobsActive).
		Concurrency(z.metadata.Concurrency).
		PollInterval(z.metadata.PollInterval.Duration).
		PollThreshold(z.metadata.PollThreshold).
		FetchVariables(zeebe.VariableStringToArray(z.metadata.FetchVariables)...).
		Open()
}

func (h *jobHandler) handleJob(client worker.JobClient, job entities.Job) {
	headers, err := job.GetCustomHeadersAsMap()
	if err != nil {
		h.failJob(client, job, err)

		return
	}

	resultVariables, err := h.callback(&bindings.ReadResponse{
		Data:     []byte(job.Variables),
		Metadata: headers,
	})
	if err != nil {
		h.failJob(client, job, err)

		return
	}

	variablesMap := make(map[string]interface{})
	err = json.Unmarshal(resultVariables, &variablesMap)
	if err != nil {
		h.failJob(client, job, fmt.Errorf("cannot parse variables from binding result %s; got error %w", string(resultVariables), err))

		return
	}

	jobKey := job.GetKey()
	request, err := client.NewCompleteJobCommand().JobKey(jobKey).VariablesFromMap(variablesMap)
	if err != nil {
		h.failJob(client, job, err)

		return
	}

	h.logger.Debugf("Complete job %s of type %s", jobKey, job.Type)

	ctx := context.Background()
	_, err = request.Send(ctx)
	if err != nil {
		panic(err)
	}

	h.logger.Debug("Successfully completed job")
}

func (h *jobHandler) failJob(client worker.JobClient, job entities.Job, reason error) {
	h.logger.Errorf("Failed to complete job `%s` reason: %w", job.GetKey(), reason)

	ctx := context.Background()
	_, err := client.NewFailJobCommand().JobKey(job.GetKey()).Retries(job.Retries - 1).Send(ctx)
	if err != nil {
		panic(err)
	}
}
