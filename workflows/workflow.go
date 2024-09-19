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

package workflows

import (
	"context"
	"errors"
	"io"
)

var ErrNotImplemented = errors.New("this component doesn't implement the current API operation")

// Workflow is an interface to perform operations on Workflow.
type Workflow interface {
	Init(metadata Metadata) error
	Start(ctx context.Context, req *StartRequest) (*StartResponse, error)
	Terminate(ctx context.Context, req *TerminateRequest) error
	Get(ctx context.Context, req *GetRequest) (*StateResponse, error)
	RaiseEvent(ctx context.Context, req *RaiseEventRequest) error
	Purge(ctx context.Context, req *PurgeRequest) error
	Pause(ctx context.Context, req *PauseRequest) error
	Resume(ctx context.Context, req *ResumeRequest) error
	io.Closer
}
