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

// Lock acquire request was successful or not.
type TryLockResponse struct {
	Success bool
}

// Status when releasing the lock.
type UnlockResponse struct {
	Status Status
}

type Status int32

// lock status.
const (
	Success            Status = 0
	LockUnexist        Status = 1
	LockBelongToOthers Status = 2
	InternalError      Status = 3
)
