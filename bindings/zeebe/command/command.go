/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package command

import (
	"errors"
	"fmt"

	"github.com/camunda-cloud/zeebe/clients/go/pkg/zbc"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe"
	"github.com/dapr/kit/logger"
)

const (
	// operations.
	TopologyOperation         bindings.OperationKind = "topology"
	DeployProcessOperation    bindings.OperationKind = "deploy-process"
	CreateInstanceOperation   bindings.OperationKind = "create-instance"
	CancelInstanceOperation   bindings.OperationKind = "cancel-instance"
	SetVariablesOperation     bindings.OperationKind = "set-variables"
	ResolveIncidentOperation  bindings.OperationKind = "resolve-incident"
	PublishMessageOperation   bindings.OperationKind = "publish-message"
	ActivateJobsOperation     bindings.OperationKind = "activate-jobs"
	CompleteJobOperation      bindings.OperationKind = "complete-job"
	FailJobOperation          bindings.OperationKind = "fail-job"
	UpdateJobRetriesOperation bindings.OperationKind = "update-job-retries"
	ThrowErrorOperation       bindings.OperationKind = "throw-error"
)

var (
	ErrMissingJobKey        = errors.New("jobKey is a required attribute")
	ErrUnsupportedOperation = func(operation bindings.OperationKind) error {
		return fmt.Errorf("unsupported operation: %v", operation)
	}
)

// ZeebeCommand executes Zeebe commands.
type ZeebeCommand struct {
	clientFactory zeebe.ClientFactory
	client        zbc.Client
	logger        logger.Logger
}

// NewZeebeCommand returns a new ZeebeCommand instance.
func NewZeebeCommand(logger logger.Logger) *ZeebeCommand {
	return &ZeebeCommand{clientFactory: zeebe.NewClientFactoryImpl(logger), logger: logger}
}

// Init does metadata parsing and connection creation.
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
		TopologyOperation,
		DeployProcessOperation,
		CreateInstanceOperation,
		CancelInstanceOperation,
		SetVariablesOperation,
		ResolveIncidentOperation,
		PublishMessageOperation,
		ActivateJobsOperation,
		CompleteJobOperation,
		FailJobOperation,
		UpdateJobRetriesOperation,
		ThrowErrorOperation,
	}
}

func (z *ZeebeCommand) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case TopologyOperation:
		return z.topology()
	case DeployProcessOperation:
		return z.deployProcess(req)
	case CreateInstanceOperation:
		return z.createInstance(req)
	case CancelInstanceOperation:
		return z.cancelInstance(req)
	case SetVariablesOperation:
		return z.setVariables(req)
	case ResolveIncidentOperation:
		return z.resolveIncident(req)
	case PublishMessageOperation:
		return z.publishMessage(req)
	case ActivateJobsOperation:
		return z.activateJobs(req)
	case CompleteJobOperation:
		return z.completeJob(req)
	case FailJobOperation:
		return z.failJob(req)
	case UpdateJobRetriesOperation:
		return z.updateJobRetries(req)
	case ThrowErrorOperation:
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
		return nil, ErrUnsupportedOperation(req.Operation)
	}
}
