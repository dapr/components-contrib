/*
Copyright 2026 The Dapr Authors
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

package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// ErrorDomain is the google.rpc.ErrorInfo domain used by search and vector
	// errors.
	ErrorDomain = "dapr.io"

	// ReasonContinuationExpired is set when a provider cursor referenced by a
	// continuation token has expired. Returned with FAILED_PRECONDITION.
	ReasonContinuationExpired = "SEARCH_CONTINUATION_EXPIRED"

	// ReasonIndexingOutcomeUnknown is set when a provider reports a batch-level
	// failure but cannot establish which items were applied.
	ReasonIndexingOutcomeUnknown = "INDEXING_OUTCOME_UNKNOWN"
)

// IndexAck is the acknowledgement boundary reached by a write. Mirrors
// search.proto IndexAck.
type IndexAck int32

const (
	// IndexAckUnspecified is never returned by a successful write.
	IndexAckUnspecified IndexAck = 0
	// IndexAckQueued means the provider accepted the request for asynchronous
	// processing. It does not indicate that any item was applied.
	IndexAckQueued IndexAck = 1
	// IndexAckCompleted means the provider completed the write and
	// FailedItems contains every item-specific failure.
	IndexAckCompleted IndexAck = 2
)

// IndexingMode selects the acknowledgement boundary a write returns at.
type IndexingMode int32

const (
	// IndexingModeUnspecified behaves exactly as IndexingModeReturnOnAcceptance.
	IndexingModeUnspecified IndexingMode = 0
	// IndexingModeWaitForCompletion waits for a final provider result.
	IndexingModeWaitForCompletion IndexingMode = 1
	// IndexingModeReturnOnAcceptance returns after the provider durably
	// accepts the write for background processing.
	IndexingModeReturnOnAcceptance IndexingMode = 2
)

// IndexingWaitTimeoutAction selects what happens when WaitTimeout expires.
type IndexingWaitTimeoutAction int32

const (
	IndexingWaitTimeoutActionUnspecified IndexingWaitTimeoutAction = 0
	// IndexingWaitTimeoutActionContinueAsync returns IndexAckQueued and lets
	// the durably queued provider task continue.
	IndexingWaitTimeoutActionContinueAsync IndexingWaitTimeoutAction = 1
	// IndexingWaitTimeoutActionFailRequest returns DEADLINE_EXCEEDED.
	IndexingWaitTimeoutActionFailRequest IndexingWaitTimeoutAction = 2
)

// IndexingOptions mirrors search.proto IndexingOptionsAlpha1.
type IndexingOptions struct {
	Mode          IndexingMode
	WaitTimeout   time.Duration
	OnWaitTimeout IndexingWaitTimeoutAction
}

// ValidateIndexingOptions validates the portable IndexingOptions rules.
// supportsQueuedAck reports whether the provider offers a native durable
// queued acknowledgement; CONTINUE_ASYNC is rejected when it does not.
// Returned errors are INVALID_ARGUMENT status errors.
func ValidateIndexingOptions(ctx context.Context, opts IndexingOptions, supportsQueuedAck bool) error {
	switch opts.Mode {
	case IndexingModeUnspecified, IndexingModeReturnOnAcceptance:
		if opts.WaitTimeout != 0 || opts.OnWaitTimeout != IndexingWaitTimeoutActionUnspecified {
			return status.Error(codes.InvalidArgument, "wait_timeout and on_wait_timeout are only valid with INDEXING_MODE_WAIT_FOR_COMPLETION")
		}
		return nil
	case IndexingModeWaitForCompletion:
	default:
		return status.Errorf(codes.InvalidArgument, "unknown indexing mode %d", opts.Mode)
	}

	if opts.WaitTimeout <= 0 {
		return status.Error(codes.InvalidArgument, "wait_timeout must be positive with INDEXING_MODE_WAIT_FOR_COMPLETION")
	}
	switch opts.OnWaitTimeout {
	case IndexingWaitTimeoutActionFailRequest:
	case IndexingWaitTimeoutActionContinueAsync:
		if !supportsQueuedAck {
			return status.Error(codes.InvalidArgument, "INDEXING_WAIT_TIMEOUT_ACTION_CONTINUE_ASYNC requires a provider with a queued acknowledgement")
		}
	case IndexingWaitTimeoutActionUnspecified:
		return status.Error(codes.InvalidArgument, "on_wait_timeout is required with INDEXING_MODE_WAIT_FOR_COMPLETION")
	default:
		return status.Errorf(codes.InvalidArgument, "unknown on_wait_timeout action %d", opts.OnWaitTimeout)
	}

	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= opts.WaitTimeout {
		return status.Error(codes.InvalidArgument, "the remaining request deadline must be longer than wait_timeout")
	}
	return nil
}

// ValidateWriteIDs checks that every ID of a keyed upsert is non-empty and
// unique within the request. Returned errors are INVALID_ARGUMENT status
// errors.
func ValidateWriteIDs(ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for i, id := range ids {
		if id == "" {
			return status.Errorf(codes.InvalidArgument, "item %d has an empty id", i)
		}
		if _, ok := seen[id]; ok {
			return status.Errorf(codes.InvalidArgument, "duplicate id %q in request", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

// ValidateDocumentContent checks that content is a UTF-8 encoded JSON object,
// the normative alpha document encoding. The returned error is an
// INVALID_ARGUMENT status suitable for a FailedItem.
func ValidateDocumentContent(content []byte) error {
	trimmed := bytes.TrimSpace(content)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return status.Error(codes.InvalidArgument, "content must be a JSON object")
	}
	return nil
}

// NewErrorWithReason returns a status error with a google.rpc.ErrorInfo
// detail carrying reason.
func NewErrorWithReason(code codes.Code, reason, msg string) error {
	st := status.New(code, msg)
	if withDetails, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: ErrorDomain}); err == nil {
		st = withDetails
	}
	return st.Err()
}

// ContinuationExpiredError returns the FAILED_PRECONDITION error used when a
// provider cursor has expired.
func ContinuationExpiredError(msg string) error {
	return NewErrorWithReason(codes.FailedPrecondition, ReasonContinuationExpired, msg)
}

// IndexingOutcomeUnknownError wraps a batch-level provider failure whose
// per-item outcome cannot be established.
func IndexingOutcomeUnknownError(code codes.Code, msg string) error {
	if code == codes.OK {
		code = codes.Unknown
	}
	return NewErrorWithReason(code, ReasonIndexingOutcomeUnknown, fmt.Sprintf("indexing outcome unknown: %s", msg))
}
