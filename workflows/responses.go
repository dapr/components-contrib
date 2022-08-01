package workflows

import "go.temporal.io/api/enums/v1"

type StateResponse struct {
	WfInfo    WorkflowStruct
	StartTime string                        `json:"start_time"`
	TaskQueue string                        `json:"task_queue"`
	Status    enums.WorkflowExecutionStatus `json:"status"`
}
