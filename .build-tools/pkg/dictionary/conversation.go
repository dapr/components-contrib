/* Copyright 2024 The Dapr Authors
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

package dictionary

import "github.com/dapr/components-contrib/build-tools/pkg/metadataschema"

func GetAllConversation() []*metadataschema.ComponentMetadata {
	return []*metadataschema.ComponentMetadata{
		&conversationAnthropic,
		&conversationBedrock,
		&conversationHugging,
		&conversationMistral,
		&conversationOpenAI,
	}
}

var conversationAnthropic = metadataschema.ComponentMetadata{
	Type:         conversation,
	Name:         nameAnthropic,
	Version:      v1,
	Status:       alpha,
	Title:        titleAnthropic,
	Description:  "", // TODO
	Capabilities: []string{},
}

var conversationBedrock = metadataschema.ComponentMetadata{
	Type:         conversation,
	Name:         nameBedrock,
	Version:      v1,
	Status:       alpha,
	Title:        titleBedrock,
	Description:  "", // TODO
	Capabilities: []string{},
}

var conversationHugging = metadataschema.ComponentMetadata{
	Type:         conversation,
	Name:         nameHugging,
	Version:      v1,
	Status:       alpha,
	Title:        titleHugging,
	Description:  "", // TODO
	Capabilities: []string{},
}

var conversationMistral = metadataschema.ComponentMetadata{
	Type:         conversation,
	Name:         nameHugging,
	Version:      v1,
	Status:       alpha,
	Title:        titleMistral,
	Description:  "", // TODO
	Capabilities: []string{},
}

var conversationOpenAI = metadataschema.ComponentMetadata{
	Type:         conversation,
	Name:         nameOpenAI,
	Version:      v1,
	Status:       alpha,
	Title:        titleOpenAI,
	Description:  "", // TODO
	Capabilities: []string{},
}
