package workflows

import "time"

type WorkflowState struct {
	InstanceID    string            `json:"instanceID"`
	WorkflowName  string            `json:"workflowName"`
	CreatedAt     time.Time         `json:"startedAt"`
	LastUpdatedAt time.Time         `json:"lastUpdatedAt"`
	RuntimeStatus string            `json:"runtimeStatus"`
	Properties    map[string]string `json:"properties"`
}

type StartResponse struct {
	InstanceID string `json:"instanceID"`
}

type StateResponse struct {
	Workflow *WorkflowState `json:"workflow"`
}
