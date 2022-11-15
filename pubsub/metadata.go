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

package pubsub

import "github.com/dapr/components-contrib/metadata"

// Metadata represents a set of message-bus specific properties.
type Metadata struct {
	metadata.Base `json:",inline"`
}

// When the Dapr component does not explicitly specify a consumer group,
// this value provided by the runtime must be used. This value is specific to each Dapr App.
// As a result, by default, each Dapr App will receive all messages published to the topic at least once.
// See https://github.com/dapr/dapr/blob/21566de8d7fdc7d43ae627ffc0698cc073fa71b0/pkg/runtime/runtime.go#L1735-L1739
const RuntimeConsumerIDKey = "consumerID"
