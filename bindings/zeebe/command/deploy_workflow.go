// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/dapr/components-contrib/bindings"
	"github.com/zeebe-io/zeebe/clients/go/pkg/pb"
)

const (
	// metadata
	fileName = "fileName"
	fileType = "fileType"

	// errors
	missingFileNameErrorMsg = "fileName is a required attribute"
	missingFileTypeErrorMsg = "cannot determine file type from file name. Please specify a fileType"
	invalidFileTypeErrorMsg = "fileType must be either 'bpmn' of 'file'"
)

func (z *ZeebeCommand) deployWorkflow(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var deployFileName string
	var deployFileType string

	if val, ok := req.Metadata[fileName]; ok && val != "" {
		deployFileName = val
	} else {
		return nil, errors.New(missingFileNameErrorMsg)
	}

	if val, ok := req.Metadata[fileType]; ok && val != "" {
		deployFileType = val
	} else {
		var extension = filepath.Ext(deployFileName)
		if extension == "" {
			return nil, errors.New(missingFileTypeErrorMsg)
		}

		if extension == ".bpmn" {
			deployFileType = "bpmn"
		} else {
			deployFileType = "file"
		}
	}

	var resourceType pb.WorkflowRequestObject_ResourceType
	if deployFileType == "bpmn" {
		resourceType = pb.WorkflowRequestObject_BPMN
	} else if deployFileType == "file" {
		resourceType = pb.WorkflowRequestObject_FILE
	} else {
		return nil, errors.New(invalidFileTypeErrorMsg)
	}

	response, err := z.client.NewDeployWorkflowCommand().
		AddResource(req.Data, deployFileName, resourceType).
		Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot deploy workflow with fileName %s: %s", deployFileName, err)
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal response to json: %s", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}
