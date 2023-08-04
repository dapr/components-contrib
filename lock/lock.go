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

package lock

import (
	"context"

	"github.com/dapr/components-contrib/internal/features"
	"github.com/dapr/components-contrib/metadata"
)

// Store is the interface for lock stores.
type Store interface {
	metadata.ComponentWithMetadata

	// Init the lock store.
	Init(ctx context.Context, metadata Metadata) error

	// Features returns the list of supported features.
	Features() []Feature

	// Lock acquires a lock.
	// If the lock is owned by someone else, this method blocks until the lock can be acquired or the context is canceled.
	Lock(ctx context.Context, req *LockRequest) (*LockResponse, error)

	// TryLock tries to acquire a lock.
	// If the lock cannot be acquired, it returns immediately.
	TryLock(ctx context.Context, req *LockRequest) (*LockResponse, error)

	// RenewLock attempts to renew a lock if the lock is still valid.
	RenewLock(ctx context.Context, req *RenewLockRequest) (*RenewLockResponse, error)

	// Unlock tries to release a lock if the lock is still valid.
	Unlock(ctx context.Context, req *UnlockRequest) (*UnlockResponse, error)
}

// Metadata contains a lock store specific set of metadata property.
type Metadata struct {
	metadata.Base `json:",inline"`
}

// Feature names a feature that can be implemented by the lock stores.
type Feature = features.Feature[Store]

// LockRequest is the request to acquire locks, used by Lock and TryLock.
type LockRequest struct {
	ResourceID      string `json:"resourceId"`
	LockOwner       string `json:"lockOwner"`
	ExpiryInSeconds int32  `json:"expiryInSeconds"`
}

// LockResponse is the response used by Lock and TryLock when the operation is completed.
type LockResponse struct {
	Success bool `json:"success"`
}

// RenewLockRequest is a lock renewal request.
type RenewLockRequest struct {
	ResourceID      string `json:"resourceId"`
	LockOwner       string `json:"lockOwner"`
	ExpiryInSeconds int32  `json:"expiryInSeconds"`
}

// RenewLockResponse is a lock renewal request.
type RenewLockResponse struct {
	Status LockStatus `json:"status"`
}

// UnlockRequest is a lock release request.
type UnlockRequest struct {
	ResourceID string `json:"resourceId"`
	LockOwner  string `json:"lockOwner"`
}

// Status when releasing the lock.
type UnlockResponse struct {
	Status LockStatus `json:"status"`
}

// LockStatus is the status included in lock responses.
type LockStatus int32

// lock status.
const (
	LockStatusInternalError LockStatus = -1
	LockStatusSuccess       LockStatus = 0
	LockStatusNotExist      LockStatus = 1
	LockStatusOwnerMismatch LockStatus = 2
)
