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

package configuration

// Item represents a configuration item with name, content and other information.
type Item struct {
	Key      string            `json:"key"`
	Value    string            `json:"value,omitempty"`
	Version  string            `json:"version,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// GetRequest is the object describing a request to get configuration.
type GetRequest struct {
	Keys     []string          `json:"keys"`
	Metadata map[string]string `json:"metadata"`
}

// SubscribeRequest is the object describing a request to subscribe configuration.
type SubscribeRequest struct {
	Keys     []string          `json:"keys"`
	Metadata map[string]string `json:"metadata"`
}

// UnsubscribeRequest is the object describing a request to unsubscribe configuration.
type UnsubscribeRequest struct {
	ID string `json:"id"`
}

// UpdateEvent is the object describing a configuration update event.
type UpdateEvent struct {
	ID    string  `json:"id"`
	Items []*Item `json:"items"`
}
