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

package pubsub

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CodeError wraps a Publish/BulkPublish error with a gRPC status code so the Dapr
// runtime's resiliency layer can classify it for retry matching. It implements
// GRPCStatus() (so status.FromError reads the code) and Unwrap() (so errors.Is/As
// reach the underlying broker error).
type CodeError struct {
	code codes.Code
	err  error
}

// Error implements the error interface, returning the wrapped error's message.
func (e *CodeError) Error() string {
	return e.err.Error()
}

// Unwrap returns the wrapped error so errors.Is/errors.As work through a CodeError.
func (e *CodeError) Unwrap() error {
	return e.err
}

// GRPCStatus returns the gRPC status carrying the classification code. This is what
// status.FromError (used by the runtime) reads to obtain the code.
func (e *CodeError) GRPCStatus() *status.Status {
	return status.New(e.code, e.err.Error())
}

// Code returns the gRPC status code assigned to this error.
func (e *CodeError) Code() codes.Code {
	return e.code
}

// withCode attaches code to err, unless err already carries a gRPC status —
// in which case the precise existing code is preserved rather than overridden.
func withCode(code codes.Code, err error) error {
	if err == nil {
		return nil
	}
	// err already implements a gRPC status
	if _, ok := status.FromError(err); ok {
		return err
	}
	return &CodeError{code: code, err: err}
}

// NewRetriableError marks a publish error as transient (codes.Unavailable) unless it
// already carries a gRPC status, in which case that code is preserved. Returns nil if
// err is nil. With no retry `matching` configured the runtime retries every error
// regardless of code, so this is behavior-preserving until a user configures `matching`.
func NewRetriableError(err error) error {
	return withCode(codes.Unavailable, err)
}

// NewTerminalError marks a publish error as non-retriable (codes.FailedPrecondition)
// unless it already carries a gRPC status, in which case that code is preserved. A user
// can exclude the code from retry `matching` to stop retries. Returns nil if err is nil.
func NewTerminalError(err error) error {
	return withCode(codes.FailedPrecondition, err)
}
