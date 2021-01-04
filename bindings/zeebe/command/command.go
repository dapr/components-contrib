// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"fmt"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"
)

const (
	// errors
	unsupportedOperationErrorMsg = "unsupported operation: %v"
	missingJobKeyErrorMsg        = "jobKey is a required attribute"

	// operations
	topologyOperation         bindings.OperationKind = "topology"
	deployWorkflowOperation   bindings.OperationKind = "deploy-workflow"
	createInstanceOperation   bindings.OperationKind = "create-instance"
	cancelInstanceOperation   bindings.OperationKind = "cancel-instance"
	setVariablesOperation     bindings.OperationKind = "set-variables"
	resolveIncidentOperation  bindings.OperationKind = "resolve-incident"
	publishMessageOperation   bindings.OperationKind = "publish-message"
	activateJobsOperation     bindings.OperationKind = "activate-jobs"
	completeJobOperation      bindings.OperationKind = "complete-job"
	failJobOperation          bindings.OperationKind = "fail-job"
	updateJobRetriesOperation bindings.OperationKind = "update-job-retries"
	throwErrorOperation       bindings.OperationKind = "throw-error"
)

// ZeebeCommand executes Zeebe commands
type ZeebeCommand struct {
	clientFactory zeebe.ClientFactory
	client        zbc.Client
	logger        logger.Logger
}

// NewZeebeCommand returns a new ZeebeCommand instance
func NewZeebeCommand(logger logger.Logger) *ZeebeCommand {
	return &ZeebeCommand{clientFactory: zeebe.NewClientFactoryImpl(logger), logger: logger}
}

// Init does metadata parsing and connection creation
func (z *ZeebeCommand) Init(metadata bindings.Metadata) error {
	client, err := z.clientFactory.Get(metadata)
	if err != nil {
		return err
	}

	z.client = client

	return nil
}

func (z *ZeebeCommand) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		topologyOperation,
		deployWorkflowOperation,
		createInstanceOperation,
		cancelInstanceOperation,
		setVariablesOperation,
		resolveIncidentOperation,
		publishMessageOperation,
		activateJobsOperation,
		completeJobOperation,
		failJobOperation,
		updateJobRetriesOperation,
		throwErrorOperation,
	}
}

func (z *ZeebeCommand) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case topologyOperation:
		return z.topology()
	case deployWorkflowOperation:
		return z.deployWorkflow(req)
	case createInstanceOperation:
		return z.createInstance(req)
	case cancelInstanceOperation:
		return z.cancelInstance(req)
	case setVariablesOperation:
		return z.setVariables(req)
	case resolveIncidentOperation:
		return z.resolveIncident(req)
	case publishMessageOperation:
		return z.publishMessage(req)
	case activateJobsOperation:
		return z.activateJobs(req)
	case completeJobOperation:
		return z.completeJob(req)
	case failJobOperation:
		return z.failJob(req)
	case updateJobRetriesOperation:
		return z.updateJobRetries(req)
	case throwErrorOperation:
		return z.throwError(req)
	case bindings.GetOperation:
		fallthrough
	case bindings.CreateOperation:
		fallthrough
	case bindings.DeleteOperation:
		fallthrough
	case bindings.ListOperation:
		fallthrough
	default:
		return nil, fmt.Errorf(unsupportedOperationErrorMsg, req.Operation)
	}
}
