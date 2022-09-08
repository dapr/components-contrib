# Workflows

A workflow is custom application logic that consists of a set of tasks and or state transitions.

## Implementing a new Workflow

A compliant workflow needs to implement the `Workflow` inteface included in the [`workflow.go`](workflow.go) file.

## Using Temporal

When using temporal as the workflow, the task queue must be provided as an Option in the start request struct with the key: `task_queue`