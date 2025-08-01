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

// TryLockRequest is a lock acquire request.
type TryLockRequest struct {
	ResourceID      string            `json:"resourceId"`
	LockOwner       string            `json:"lockOwner"`
	ExpiryInSeconds int32             `json:"expiryInSeconds"`
	Metadata        map[string]string `json:"metadata"`
}

// UnlockRequest is a lock release request.
type UnlockRequest struct {
	ResourceID string            `json:"resourceId"`
	LockOwner  string            `json:"lockOwner"`
	Metadata   map[string]string `json:"metadata"`
}
