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

package jobworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/camunda/zeebe/clients/go/v8/pkg/entities"
	"github.com/camunda/zeebe/clients/go/v8/pkg/worker"
	"github.com/camunda/zeebe/clients/go/v8/pkg/zbc"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

var ErrMissingJobType = errors.New("jobType is a required attribute")

// ZeebeJobWorker allows handling jobs from the Zeebe command engine.
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
	Autocomplete   *bool             `json:"autocomplete,omitempty"`
}

type jobHandler struct {
	callback     bindings.Handler
	logger       logger.Logger
	ctx          context.Context
	autocomplete bool
}

// NewZeebeJobWorker returns a new ZeebeJobWorker instance.
func NewZeebeJobWorker(logger logger.Logger) bindings.InputBinding {
	return &ZeebeJobWorker{clientFactory: zeebe.NewClientFactoryImpl(logger), logger: logger}
}

// Init does metadata parsing and connection creation.
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

func (z *ZeebeJobWorker) Read(ctx context.Context, handler bindings.Handler) error {
	h := jobHandler{
		callback:     handler,
		logger:       z.logger,
		ctx:          ctx,
		autocomplete: z.metadata.Autocomplete == nil || *z.metadata.Autocomplete,
	}

	jobWorker := z.getJobWorker(h)

	go func() {
		<-ctx.Done()

		jobWorker.Close()
		jobWorker.AwaitClose()
		z.client.Close()
	}()

	return nil
}

func (z *ZeebeJobWorker) parseMetadata(meta bindings.Metadata) (*jobWorkerMetadata, error) {
	var m jobWorkerMetadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (z *ZeebeJobWorker) getJobWorker(handler jobHandler) worker.JobWorker {
	cmd1 := z.client.NewJobWorker()
	cmd2 := cmd1.JobType(z.metadata.JobType)
	cmd3 := cmd2.Handler(handler.handleJob)
	if z.metadata.WorkerName != "" {
		cmd3 = cmd3.Name(z.metadata.WorkerName)
	}
	if z.metadata.WorkerTimeout.Duration != time.Duration(0) {
		cmd3 = cmd3.Timeout(z.metadata.WorkerTimeout.Duration)
	}
	if z.metadata.RequestTimeout.Duration != time.Duration(0) {
		cmd3 = cmd3.RequestTimeout(z.metadata.RequestTimeout.Duration)
	}
	if z.metadata.MaxJobsActive != 0 {
		cmd3 = cmd3.MaxJobsActive(z.metadata.MaxJobsActive)
	}
	if z.metadata.Concurrency != 0 {
		cmd3 = cmd3.Concurrency(z.metadata.Concurrency)
	}
	if z.metadata.PollInterval.Duration != time.Duration(0) {
		cmd3 = cmd3.PollInterval(z.metadata.PollInterval.Duration)
	}
	if z.metadata.PollThreshold != 0 {
		cmd3 = cmd3.PollThreshold(z.metadata.PollThreshold)
	}
	if z.metadata.FetchVariables != "" {
		cmd3 = cmd3.FetchVariables(zeebe.VariableStringToArray(z.metadata.FetchVariables)...)
	}

	return cmd3.Open()
}

func (h *jobHandler) handleJob(client worker.JobClient, job entities.Job) {
	headers, err := job.GetCustomHeadersAsMap()
	if err != nil {
		// Use a background context because the subscription one may be canceled
		h.failJob(context.Background(), client, job, err)
		return
	}

	headers["X-Zeebe-Job-Key"] = strconv.FormatInt(job.Key, 10)
	headers["X-Zeebe-Job-Type"] = job.Type
	headers["X-Zeebe-Process-Instance-Key"] = strconv.FormatInt(job.ProcessInstanceKey, 10)
	headers["X-Zeebe-Bpmn-Process-Id"] = job.BpmnProcessId
	headers["X-Zeebe-Process-Definition-Version"] = strconv.FormatInt(int64(job.ProcessDefinitionVersion), 10)
	headers["X-Zeebe-Process-Definition-Key"] = strconv.FormatInt(job.ProcessDefinitionKey, 10)
	headers["X-Zeebe-Element-Id"] = job.ElementId
	headers["X-Zeebe-Element-Instance-Key"] = strconv.FormatInt(job.ElementInstanceKey, 10)
	headers["X-Zeebe-Worker"] = job.Worker
	headers["X-Zeebe-Retries"] = strconv.FormatInt(int64(job.Retries), 10)
	headers["X-Zeebe-Deadline"] = strconv.FormatInt(job.Deadline, 10)

	resultVariables, err := h.callback(h.ctx, &bindings.ReadResponse{
		Data:     []byte(job.Variables),
		Metadata: headers,
	})
	if err != nil {
		// Use a background context because the subscription one may be canceled
		h.failJob(context.Background(), client, job, err)
		return
	}

	jobKey := job.GetKey()
	if h.autocomplete {
		variablesMap := make(map[string]interface{})
		if resultVariables != nil {
			err = json.Unmarshal(resultVariables, &variablesMap)
			if err != nil {
				// Use a background context because the subscription one may be canceled
				h.failJob(context.Background(), client, job, fmt.Errorf("cannot parse variables from binding result %s; got error %w", string(resultVariables), err))
				return
			}
		}

		request, err := client.NewCompleteJobCommand().JobKey(jobKey).VariablesFromMap(variablesMap)
		if err != nil {
			// Use a background context because the subscription one may be canceled
			h.failJob(context.Background(), client, job, err)
			return
		}

		h.logger.Debugf("Complete job `%d` of type `%s`", jobKey, job.Type)

		// Use a background context because the subscription one may be canceled
		_, err = request.Send(context.Background())
		if err != nil {
			h.logger.Errorf("Cannot complete job `%d` of type `%s`; got error: %s", jobKey, job.Type, err.Error())
			return
		}

		h.logger.Debugf("Successfully completed job `%d` of type `%s`", jobKey, job.Type)
	} else {
		h.logger.Debugf("Auto-completion for job `%d` of type `%s` is disabled. Use the job commands to complete the job from the worker", jobKey, job.Type)
	}
}

func (h *jobHandler) failJob(ctx context.Context, client worker.JobClient, job entities.Job, reason error) {
	reasonMsg := reason.Error()
	h.logger.Errorf("Failed to complete job `%d` reason: %s", job.GetKey(), reasonMsg)

	_, err := client.NewFailJobCommand().JobKey(job.GetKey()).Retries(job.Retries - 1).ErrorMessage(reasonMsg).Send(ctx)
	if err != nil {
		h.logger.Errorf("Cannot fail job `%d` of type `%s`; got error: %s", job.GetKey(), job.Type, err.Error())

		return
	}
}
