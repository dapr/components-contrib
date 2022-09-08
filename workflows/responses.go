package workflows

type StateResponse struct {
	WFInfo    WorkflowReference
	StartTime string `json:"start_time"`
	TaskQueue string `json:"task_queue"`
	Status    string `json:"status"`
}
