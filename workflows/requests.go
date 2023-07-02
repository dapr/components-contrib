package workflows

// StartRequest is the struct describing a start workflow request.
type StartRequest struct {
	InstanceID    string            `json:"instanceID"`
	Options       map[string]string `json:"options"`
	WorkflowName  string            `json:"workflowName"`
	WorkflowInput []byte            `json:"workflowInput"`
}

// GetRequest is the struct describing a get workflow state request.
type GetRequest struct {
	InstanceID string `json:"instanceID"`
}

// TerminateRequest is the struct describing a terminate workflow request.
type TerminateRequest struct {
	InstanceID string `json:"instanceID"`
}

// RaiseEventRequest is the struct describing a raise workflow event request.
type RaiseEventRequest struct {
	InstanceID string `json:"instanceID"`
	EventName  string `json:"name"`
	EventData  []byte `json:"data"`
}

// PauseRequest is the struct describing a pause workflow request.
type PauseRequest struct {
	InstanceID string `json:"instanceID"`
}

// ResumeRequest is the struct describing a resume workflow request.
type ResumeRequest struct {
	InstanceID string `json:"instanceID"`
}

// PurgeRequest is the object describing a Purge request.
type PurgeRequest struct {
	InstanceID string `json:"instanceID"`
}
