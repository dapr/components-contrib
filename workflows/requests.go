package workflows

type WorkflowReference struct {
	InstanceID string `json:"instance_id"`
}

// StartRequest is the object describing a Start Workflow request.
type StartRequest struct {
	Options           map[string]string `json:"workflow_options"`
	WorkflowReference WorkflowReference `json:"workflow_reference"`
	WorkflowName      string            `json:"function_name"`
	Input             interface{}       `json:"input"`
}
