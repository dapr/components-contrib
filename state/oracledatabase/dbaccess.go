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

package oracledatabase

import (
	"github.com/dapr/components-contrib/state"
)

// dbAccess is a private interface which enables unit testing of Oracle Database.
type dbAccess interface {
	Init(metadata state.Metadata) error
	Ping() error
	Set(req *state.SetRequest) error
	Get(req *state.GetRequest) (*state.GetResponse, error)
	Delete(req *state.DeleteRequest) error
	ExecuteMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error
	Close() error // io.Closer.
}
