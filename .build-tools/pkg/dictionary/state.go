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

func GetAllStateStores() []*metadataschema.ComponentMetadata {
	return []*metadataschema.ComponentMetadata{
		&stateDynamo,
		&stateBlobV1,
		&stateBlobV2,
		&stateCosmos,
		&stateTableStorage,
		&stateCassandra,
		&stateCloudflareWorkers,
		&stateCockroach,
		&stateInMem,
		&stateMemCached,
		&stateMongo,
		&stateMySQL,
		&statePostgresV1,
		&statePostgresV2,
		&stateRedis,
		&stateSqlServer,
	}
}

var stateDynamo = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameDynamo,
	Version:      v1,
	Status:       stable,
	Title:        titleDynamo,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesActorState, capabilitiesCrud, capabilitiesTransaction, capabilitiesEtag, capabilitiesTtl},
}

var stateBlobV1 = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameBlob,
	Version:      v1,
	Status:       stable,
	Title:        titleBlob,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesCrud, capabilitiesEtag},
}

var stateBlobV2 = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameBlob,
	Version:      v2,
	Status:       stable,
	Title:        titleBlob,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesCrud, capabilitiesEtag},
}

var stateCosmos = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameCosmosSql,
	Version:      v1,
	Status:       stable,
	Title:        titleCosmosSql,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesActorState, capabilitiesCrud, capabilitiesTransaction, capabilitiesEtag, capabilitiesTtl, capabilitiesQuery},
}

var stateTableStorage = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameTableStorage,
	Version:      v1,
	Status:       stable,
	Title:        titleTableStorage,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesCrud, capabilitiesEtag},
}

var stateCassandra = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameCassandra,
	Version:      v1,
	Status:       stable,
	Title:        titleCassandra,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesCrud, capabilitiesEtag},
}

var stateCloudflareWorkers = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameCloudflareWorkers,
	Version:      v1,
	Status:       beta,
	Title:        titleCloudflareWorkers,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesCrud, capabilitiesEtag},
}

var stateCockroach = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameCockroach,
	Version:      v1,
	Status:       stable,
	Title:        titleCockroach,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesActorState, capabilitiesCrud, capabilitiesTransaction, capabilitiesEtag, capabilitiesTtl},
}

var stateInMem = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameInMem,
	Version:      v1,
	Status:       stable,
	Title:        titleInMem,
	Description:  "", // TODO
	Capabilities: []string{},
}

var stateMemCached = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameMemcached,
	Version:      v1,
	Status:       stable,
	Title:        titleMemcached,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesCrud, capabilitiesTtl},
}

var stateMongo = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameMongo,
	Version:      v1,
	Status:       stable,
	Title:        titleMongo,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesActorState, capabilitiesCrud, capabilitiesTransaction, capabilitiesEtag, capabilitiesTtl, capabilitiesQuery},
}

var stateMySQL = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameMySQL,
	Version:      v1,
	Status:       stable,
	Title:        titleMySQL,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesActorState, capabilitiesCrud, capabilitiesTransaction, capabilitiesEtag, capabilitiesTtl},
}

var statePostgresV1 = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         namePostgresql,
	Version:      v1,
	Status:       stable,
	Title:        titlePostgresql,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesActorState, capabilitiesCrud, capabilitiesTransaction, capabilitiesEtag, capabilitiesTtl, capabilitiesQuery},
}

var statePostgresV2 = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         namePostgresql,
	Version:      v2,
	Status:       stable,
	Title:        titlePostgresql,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesActorState, capabilitiesCrud, capabilitiesTransaction, capabilitiesEtag, capabilitiesTtl},
}

var stateRedis = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameRedis,
	Version:      v1,
	Status:       stable,
	Title:        titleRedis,
	Description:  "", // TODO
	Capabilities: []string{capabilitiesActorState, capabilitiesCrud, capabilitiesTransaction, capabilitiesEtag, capabilitiesQuery},
}
var stateSqlServer = metadataschema.ComponentMetadata{
	Type:         state,
	Name:         nameSqlserver,
	Version:      v1,
	Status:       stable,
	Title:        titleSqlserver,
	Description:  descriptionSqlserver,
	Capabilities: []string{capabilitiesActorState, capabilitiesCrud, capabilitiesTransaction, capabilitiesEtag, capabilitiesTtl},
}
