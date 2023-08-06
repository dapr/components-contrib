/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implieout.
See the License for the specific language governing permissions and
limitations under the License.
*/

package errorcodes

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"
)

type Reason string

const (
	ErrorCodesFeatureMetadataKey = "error_codes_feature"
	Owner                        = "components-contrib"
	Domain                       = "dapr.io"
)

var StateETagMismatchReason = Reason("DAPR_STATE_ETAG_MISMATCH")

type ResourceInfoData struct {
	ResourceType, ResourceName string
}

func FeatureEnabled(md map[string]string) bool {
	if _, ok := md[ErrorCodesFeatureMetadataKey]; ok {
		return true
	}
	return false
}

// NewStatusError returns a Status representing Code, error Reason, Message, and optional ResourceInfo and Metadata.
// When successful, it returns a StatusError, otherwise returns the original error
func NewStatusError(code codes.Code, err error, errDescription string, reason Reason, rid *ResourceInfoData, metadata map[string]string) error {
	md := metadata
	if md == nil {
		md = map[string]string{}
	}

	messages := []protoiface.MessageV1{
		NewErrorInfo(reason, md),
	}

	if rid != nil {
		messages = append(messages, NewResourceInfo(rid, errDescription))
	}

	ste, stErr := status.New(code, errDescription).WithDetails(messages...)
	if stErr != nil {
		return err
	}

	return ste.Err()
}

func NewErrorInfo(reason Reason, md map[string]string) *errdetails.ErrorInfo {
	ei := errdetails.ErrorInfo{
		Domain:   Domain,
		Reason:   string(reason),
		Metadata: md,
	}

	return &ei
}

func NewResourceInfo(rid *ResourceInfoData, description string) *errdetails.ResourceInfo {
	return &errdetails.ResourceInfo{
		ResourceType: rid.ResourceType,
		ResourceName: rid.ResourceName,
		Owner:        Owner,
		Description:  description,
	}
}
