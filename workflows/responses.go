package workflows

type StateResponse struct {
	WfInfo    WorkflowStruct
	StartTime string `json:"start_time"`
	TaskQueue string `json:"task_queue"`
	Status    string `json:"status"`
}
