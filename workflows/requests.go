package workflows

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type StartRequestOptions struct {
	ID        string `json:"id"`
	TaskQueue string `json:"task_queue"`
}

type WorkflowStruct struct {
	WorkflowId string `json:"workflow_id"`
	InstanceId string `json:"instance_id"`
}

// StartRequest is the object describing a Start Workflow request.
type StartRequest struct {
	Options      StartRequestOptions `json:"workflow_options"`
	WorkflowName string              `json:"workflow_name"`
	Parameters   interface{}         `json:"parameters"`
}

// CreateWorkerRequest is the object describing a Create Worker request.
type CreateWorkerRequest struct {
	TaskQueue     string         `json:"task_queue"`
	WorkerOptions worker.Options `json:"worker_options"`
}

// RegisterWorkflowAndActivitiesRequest is the object describing the wfs and acts to assign to a worker
type RegisterWorkflowAndActivitiesRequest struct {
	WorkerName      string                   `json:"worker_name"`
	WorkflowFunc    interface{}              `json:"workflow_func_name"`
	ActivityFunc    interface{}              `json:"activity_func_name"`
	WorkflowOptions workflow.RegisterOptions `json:"workflow_register_options"`
	ActivityOptions activity.RegisterOptions `json:"activity_register_options"`
}
