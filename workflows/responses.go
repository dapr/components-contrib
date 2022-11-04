package workflows

type StateResponse struct {
	WFInfo    WorkflowReference
	StartTime string            `json:"start_time"`
	Metadata  map[string]string `json:"metadata"`
}
