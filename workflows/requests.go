package workflows

type WorkflowReference struct {
	InstanceID string `json:"instance_id"`
}

// StartRequest is the object describing a Start Workflow request.
type StartRequest struct {
	Options      map[string]string `json:"workflow_options"`
	InstanceID   string            `json:"workflow_reference"`
	WorkflowName string            `json:"function_name"`
	Input        interface{}       `json:"input"`
}

// RaiseEventRequest is the object describing a Raise Event request.
type RaiseEventRequest struct {
	InstanceID string `json:"workflow_reference"`
	EventName  string `json:"event_name"`
	Input      []byte `json:"input"`
}
