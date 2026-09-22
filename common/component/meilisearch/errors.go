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

package meilisearch

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	meilisearchgo "github.com/meilisearch/meilisearch-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// APIError is the error payload Meilisearch returns for a failed request or a
// failed task.
type APIError struct {
	Message string `json:"message"`
	Code    string `json:"code"`
	Type    string `json:"type"`
	Link    string `json:"link"`
}

// CodeFromHTTPStatus maps a Meilisearch HTTP status to a canonical gRPC code.
func CodeFromHTTPStatus(statusCode int) codes.Code {
	switch statusCode {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return codes.InvalidArgument
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusConflict:
		return codes.AlreadyExists
	case http.StatusRequestEntityTooLarge, http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusBadGateway:
		return codes.Unavailable
	case http.StatusInternalServerError:
		return codes.Internal
	default:
		if statusCode >= 500 {
			return codes.Internal
		}
		if statusCode >= 400 {
			return codes.InvalidArgument
		}
		return codes.Unknown
	}
}

// CodeFromAPIError maps a Meilisearch error payload to a canonical gRPC code
// using its documented error types and the most common error codes.
func CodeFromAPIError(apiErr APIError) codes.Code {
	switch apiErr.Code {
	case "index_not_found", "document_not_found", "task_not_found", "index_primary_key_no_candidate_found":
		return codes.NotFound
	case "index_already_exists":
		return codes.AlreadyExists
	case "feature_not_enabled":
		return codes.FailedPrecondition
	case "index_creation_failed", "database_size_limit_reached", "no_space_left_on_device":
		return codes.ResourceExhausted
	}
	switch apiErr.Type {
	case "invalid_request":
		return codes.InvalidArgument
	case "auth":
		return codes.Unauthenticated
	case "system":
		return codes.Unavailable
	case "internal":
		return codes.Internal
	default:
		return codes.Unknown
	}
}

// StatusError converts an error returned by the Meilisearch client into a gRPC
// status error. Context errors keep their canonical codes so a caller-driven
// cancellation is never reported as a provider failure. msg describes the
// operation and must not embed document content.
func StatusError(err error, msg string) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "%s: %v", msg, err)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "%s: %v", msg, err)
	}

	var msErr *meilisearchgo.Error
	if errors.As(err, &msErr) {
		if msErr.StatusCode > 0 {
			apiErr := APIError{
				Message: msErr.MeilisearchApiError.Message,
				Code:    msErr.MeilisearchApiError.Code,
				Type:    msErr.MeilisearchApiError.Type,
			}
			code := CodeFromAPIError(apiErr)
			if code == codes.Unknown {
				code = CodeFromHTTPStatus(msErr.StatusCode)
			}
			if apiErr.Message != "" {
				return status.Errorf(code, "%s: %s", msg, apiErr.Message)
			}
			return status.Errorf(code, "%s: meilisearch returned HTTP %d", msg, msErr.StatusCode)
		}
		// No HTTP response was received: the request never reached a
		// conclusive provider outcome.
		return status.Errorf(codes.Unavailable, "%s: %v", msg, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", msg, err)
}

// ResponseReceived reports whether the provider answered the request with an
// HTTP status. A request that failed without a response has an indeterminate
// outcome and callers report it with the INDEXING_OUTCOME_UNKNOWN reason.
func ResponseReceived(err error) bool {
	var msErr *meilisearchgo.Error
	if errors.As(err, &msErr) {
		return msErr.StatusCode > 0
	}
	return false
}

// TaskError converts the error of a failed Meilisearch task into a gRPC status
// error.
func TaskError(taskUID int64, apiErr APIError) error {
	code := CodeFromAPIError(apiErr)
	if code == codes.Unknown {
		code = codes.Internal
	}
	msg := apiErr.Message
	if msg == "" {
		msg = apiErr.Code
	}
	if msg == "" {
		msg = "no error details reported"
	}
	return status.Error(code, fmt.Sprintf("meilisearch task %d failed: %s", taskUID, msg))
}
