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

func GetAllPubsubs() []*metadataschema.ComponentMetadata {
	return []*metadataschema.ComponentMetadata{
		&pubsubSnsSqs,
		&pubsubEventHubs,
		&pubsubServiceBusQueues,
		&pubsubServiceBusTopics,
		&pubsubGCP,
		&pubsubInMem,
		&pubsubKafka,
		&pubsubMQTT,
		&pubsubPulsar,
		&pubsubRabbit,
		&pubsubRedis,
		&pubsubAmqp,
	}
}

var pubsubSnsSqs = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameSnsSqs,
	Version:      v1,
	Status:       stable,
	Title:        titleSnsSqs,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesTtl},
}

var pubsubEventHubs = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameEventHub,
	Version:      v1,
	Status:       stable,
	Title:        titleEventGrid,
	Description:  "", // TODO
	Capabilities: []string{},
}

var pubsubServiceBusQueues = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameServiceBusQueues,
	Version:      v1,
	Status:       stable,
	Title:        titleServiceBusQueues,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesTtl},
}

var pubsubServiceBusTopics = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameServiceBusTopics,
	Version:      v1,
	Status:       stable,
	Title:        titleServiceBusTopics,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesTtl},
}

var pubsubGCP = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameGCPPubsub,
	Version:      v1,
	Status:       stable,
	Title:        titleGCPPubsub,
	Description:  "", // TODO
	Capabilities: []string{},
}

var pubsubInMem = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameInMem,
	Version:      v1,
	Status:       stable,
	Title:        titleInMem,
	Description:  "", // TODO
	Capabilities: []string{},
}

var pubsubKafka = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameKafka,
	Version:      v1,
	Status:       stable,
	Title:        titleKafka,
	Description:  "", // TODO
	Capabilities: []string{},
}

var pubsubMQTT = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameMQTT,
	Version:      v1,
	Status:       stable,
	Title:        titleMQTT,
	Description:  "", // TODO
	Capabilities: []string{},
}

var pubsubPulsar = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         namePulsar,
	Version:      v1,
	Status:       stable,
	Title:        titlePulsar,
	Description:  "", // TODO
	Capabilities: []string{},
}

var pubsubRabbit = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameRabbit,
	Version:      v1,
	Status:       stable,
	Title:        titleRabbit,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesTtl},
}

var pubsubRedis = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameRedis,
	Version:      v1,
	Status:       stable,
	Title:        titleRedis,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesTtl},
}

var pubsubAmqp = metadataschema.ComponentMetadata{
	Type:         pubsub,
	Name:         nameAmqp,
	Version:      v1,
	Status:       beta,
	Title:        titleAmqp,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesTtl},
}
