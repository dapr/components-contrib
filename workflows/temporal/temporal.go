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

package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/workflows"
	"github.com/dapr/kit/logger"
)

type TemporalWF struct {
	client client.Client
	logger logger.Logger
}

type temporalMetadata struct {
	Identity  string `json:"identity"`
	HostPort  string `json:"hostport"`
	Namespace string `json:"namespace"`
}

// NewTemporalWorkflow returns a new workflow.
func NewTemporalWorkflow(logger logger.Logger) workflows.Workflow {
	s := &TemporalWF{
		logger: logger,
	}
	return s
}

func (c *TemporalWF) Init(metadata workflows.Metadata) error {
	c.logger.Debugf("Temporal init start")
	m, err := c.parseMetadata(metadata)
	if err != nil {
		return err
	}
	cOpt := client.Options{}
	if m.HostPort != "" {
		cOpt.HostPort = m.HostPort
	}
	if m.Identity != "" {
		cOpt.Identity = m.Identity
	}
	if m.Namespace != "" {
		cOpt.Namespace = m.Namespace
	}
	// Create the workflow client
	newClient, err := client.Dial(cOpt)
	if err != nil {
		return err
	}
	c.client = newClient

	return nil
}

func (c *TemporalWF) Start(ctx context.Context, req *workflows.StartRequest) (*workflows.WorkflowReference, error) {
	c.logger.Debugf("starting workflow")

	if len(req.Options) == 0 {
		c.logger.Debugf("no options provided")
		return &workflows.WorkflowReference{}, errors.New("no options provided. At the very least, a task queue is needed")
	}

	if _, ok := req.Options["task_queue"]; !ok {
		c.logger.Debugf("no task queue provided")
		return &workflows.WorkflowReference{}, errors.New("no task queue provided")
	}
	taskQ := req.Options["task_queue"]

	opt := client.StartWorkflowOptions{ID: req.WorkflowReference.InstanceID, TaskQueue: taskQ}
	run, err := c.client.ExecuteWorkflow(ctx, opt, req.WorkflowName, req.Input)
	if err != nil {
		return &workflows.WorkflowReference{}, fmt.Errorf("error executing workflow: %w", err)
	}
	wfStruct := workflows.WorkflowReference{InstanceID: run.GetID()}
	return &wfStruct, nil
}

func (c *TemporalWF) Terminate(ctx context.Context, req *workflows.WorkflowReference) error {
	c.logger.Debugf("terminating workflow")

	err := c.client.TerminateWorkflow(ctx, req.InstanceID, "", "")
	if err != nil {
		return fmt.Errorf("error terminating workflow: %w", err)
	}
	return nil
}

func (c *TemporalWF) Get(ctx context.Context, req *workflows.WorkflowReference) (*workflows.StateResponse, error) {
	c.logger.Debugf("getting workflow data")
	resp, err := c.client.DescribeWorkflowExecution(ctx, req.InstanceID, "")
	if err != nil {
		return nil, err
	}
	// Build the output struct
	outputStruct := workflows.StateResponse{
		WFInfo:    workflows.WorkflowReference{InstanceID: req.InstanceID},
		StartTime: resp.WorkflowExecutionInfo.StartTime.Format(time.RFC3339),
		Metadata: map[string]string{
			"task_queue": resp.WorkflowExecutionInfo.GetTaskQueue(),
			"status":     lookupStatus(resp.WorkflowExecutionInfo.Status),
		},
	}

	return &outputStruct, nil
}

func (c *TemporalWF) Close() {
	c.client.Close()
}

func (c *TemporalWF) parseMetadata(meta workflows.Metadata) (*temporalMetadata, error) {
	var m temporalMetadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	return &m, err
}

func lookupStatus(status enums.WorkflowExecutionStatus) string {
	switch status {
	case 0:
		return "Unspecified"
	case 1:
		return "Running"
	case 2:
		return "Completed"
	case 3:
		return "Failed"
	case 4:
		return "Canceled"
	case 5:
		return "Terminated"
	case 6:
		return "ContinuedAsNew"
	case 7:
		return "TimedOut"
	default:
		return "status unknown"
	}
}
