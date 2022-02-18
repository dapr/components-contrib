/*
Copyright 2022 The Dapr Authors
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

package bindings

// InputBinding is the interface to define a binding that triggers on incoming events.
type InputBinding interface {
	// Init passes connection and properties metadata to the binding implementation
	Init(metadata Metadata) error
	// Read is a blocking method that triggers the callback function whenever an event arrives
	Read(handler func(*ReadResponse) ([]byte, error)) error
}
