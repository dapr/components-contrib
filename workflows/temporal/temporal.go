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
	"encoding/json"

	"github.com/dapr/components-contrib/workflows"
	"github.com/dapr/kit/logger"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// Placeholder string for the task queue
const TaskQueueString = "TestTaskQueue"

type TemporalWF struct {
	client client.Client
	logger logger.Logger
}

type temporalMetaData struct {
	Identity string `json:"identity"`
	HostPort string `json:"hostport"`
}

// NewTemporalWorkflow returns a new CosmosDB state store.
func NewTemporalWorkflow(logger logger.Logger) *TemporalWF {
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
	// Create the workflow client
	newClient, err := client.Dial(cOpt)
	if err != nil {
		return err
	}
	c.client = newClient

	return nil
}

func (c *TemporalWF) Start(ctx context.Context, req *workflows.StartRequest) (*workflows.WorkflowStruct, error) {
	c.logger.Debugf("starting workflow")
	if req.Options.TaskQueue == "" {
		c.logger.Debugf("no task queue provided")
		return &workflows.WorkflowStruct{}, nil
	}
	opt := client.StartWorkflowOptions{ID: req.WorkflowInfo.WorkflowId, TaskQueue: req.Options.TaskQueue}
	run, err := c.client.ExecuteWorkflow(ctx, opt, req.WorkflowInfo.WorkflowId, req.Parameters)
	if err != nil {
		c.logger.Debugf("error when starting workflow")
		return &workflows.WorkflowStruct{}, err
	}
	wfStruct := workflows.WorkflowStruct{WorkflowId: run.GetID(), InstanceId: run.GetRunID()}
	return &wfStruct, nil
}

func (c *TemporalWF) Terminate(ctx context.Context, req *workflows.WorkflowStruct) error {
	c.logger.Debugf("terminating workflow")
	err := c.client.TerminateWorkflow(ctx, req.WorkflowId, req.InstanceId, "")
	if err != nil {
		return err
	}
	return nil
}

func (c *TemporalWF) Get(ctx context.Context, req *workflows.WorkflowStruct) (*workflows.StateResponse, error) {
	c.logger.Debugf("getting workflow data")
	resp, err := c.client.DescribeWorkflowExecution(ctx, req.WorkflowId, req.InstanceId)
	if err != nil {
		return nil, err
	}
	// Build the output struct
	outputStruct := workflows.StateResponse{
		WfInfo:    workflows.WorkflowStruct{WorkflowId: req.WorkflowId, InstanceId: req.InstanceId},
		StartTime: resp.WorkflowExecutionInfo.StartTime.String(),
		TaskQueue: resp.WorkflowExecutionInfo.GetTaskQueue(),
		Status:    lookupStatus(resp.WorkflowExecutionInfo.Status),
	}

	return &outputStruct, nil
}

func (c *TemporalWF) Close() {

	c.client.Close()
}

func (c *TemporalWF) parseMetadata(metadata workflows.Metadata) (*temporalMetaData, error) {
	connInfo := metadata.Properties
	b, err := json.Marshal(connInfo)
	if err != nil {
		return nil, err
	}

	var creds temporalMetaData
	err = json.Unmarshal(b, &creds)
	if err != nil {
		return nil, err
	}

	return &creds, nil
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
