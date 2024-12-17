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

func GetAllBindings() []*metadataschema.ComponentMetadata {
	return []*metadataschema.ComponentMetadata{
		&bindingsS3,
		&bindingsSns,
		// &bindingsBlob, // TODO: need to refactor to add to automation
		&bindingsCosmosSQL,
		&bindingsCosmosGremlin,
		&bindingsEventGrid,
		// &bindingsEventHub, // TODO: need to refactor metadata setup to support
		&bindingsOpenAI,
		// &bindingsServiceBusQueues, // TODO: need to refactor metadata setup to support
		&bindingsStorageQueues,
		&bindingsSignalR,
		&bindingsCron,
		&bindingsGCPBucket,
		&bindingsHTTP,
		&bindingsKafka,
		&bindingsMySQL,
		&bindingsPostgres,
		&bindingsRabbit,
		&bindingsRedis,
		&bindingsZeebeCommand,
		&bindingsZeebeWorker,
		&bindingsAppConfig,
		&bindingsKinesis,
		&bindingsDynamo,
	}
}

var awsAuthProfile = metadataschema.BuiltinAuthenticationProfile{Name: "aws"}
var azureAuthProfile = metadataschema.BuiltinAuthenticationProfile{Name: "azuread"}

var bindingsS3 = metadataschema.ComponentMetadata{
	Type:                          binding,
	Name:                          nameS3,
	Version:                       v1,
	Status:                        stable,
	Title:                         titleS3,
	Description:                   "", // TODO
	Capabilities:                  []string{},
	BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{awsAuthProfile},
	Binding: &metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create blob",
			},
			{
				Name:        "get",
				Description: "Get blob",
			},
			{
				Name:        "delete",
				Description: "Delete blob",
			},
			{
				Name:        "list",
				Description: "List blob",
			},
		},
	},
}

var bindingsSns = metadataschema.ComponentMetadata{
	Type:                          binding,
	Name:                          nameSns,
	Version:                       v1,
	Status:                        alpha,
	Title:                         titleSns,
	Description:                   "", // TODO
	Capabilities:                  []string{},
	BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{awsAuthProfile},
	Binding: &metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create a new subscription",
			},
		},
	},
}

var bindingsBlob = metadataschema.ComponentMetadata{
	Type:                          binding,
	Name:                          nameBlob,
	Version:                       v1,
	Status:                        stable,
	Title:                         titleBlob,
	Description:                   "", // TODO
	Capabilities:                  []string{},
	BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{azureAuthProfile},
	Binding: &metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create blob",
			},
			{
				Name:        "get",
				Description: "Get blob",
			},
			{
				Name:        "delete",
				Description: "Delete blob",
			},
			{
				Name:        "list",
				Description: "List blob",
			},
		},
	},
}

var bindingsCosmosSQL = metadataschema.ComponentMetadata{
	Type:                          binding,
	Name:                          nameCosmosSql,
	Version:                       v1,
	Status:                        stable,
	Title:                         titleCosmosSql,
	Description:                   "", // TODO
	Capabilities:                  []string{},
	BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{azureAuthProfile},
	Binding: &metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create an item",
			},
		},
	},
}

var bindingsCosmosGremlin = metadataschema.ComponentMetadata{
	Type:                          binding,
	Name:                          nameCosmosGremlin,
	Version:                       v1,
	Status:                        alpha,
	Title:                         titleCosmosGremlim,
	Description:                   "", // TODO
	Capabilities:                  []string{},
	BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{azureAuthProfile},
	Binding: &metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "query",
				Description: "Perform a query",
			},
		},
	},
}

var bindingsEventGrid = metadataschema.ComponentMetadata{
	Type:                          binding,
	Name:                          nameEventGrid,
	Version:                       v1,
	Status:                        beta,
	Title:                         titleEventGrid,
	Description:                   "", // TODO
	Capabilities:                  []string{},
	BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{azureAuthProfile},
	Binding: &metadataschema.Binding{
		Input:  true,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create an event subscription",
			},
		},
	},
}

// TODO(@Sam): I need to move the metadata.go file or create cust struct with field on location of metadata go file.
// var bindingsEventHub = metadataschema.ComponentMetadata{
// 	Type:         binding,
// 	Name:         nameEventHub,
// 	Version:      v1,
// 	Status:       stable,
// 	Title:        titleEventHub,
// 	Description:  "", // TODO
// 	Capabilities: []string{},
// }

var bindingsOpenAI = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameOpenAI,
	Version:      v1,
	Status:       alpha,
	Title:        titleOpenAI,
	Description:  "", // TODO
	Capabilities: []string{},
	AuthenticationProfiles: []metadataschema.AuthenticationProfile{
		{
			Title:       "API Key",
			Description: "Authenticate using an API key",
		},
	},
	BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{azureAuthProfile},
	Binding: &metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "completion",
				Description: "Text completion",
			},
			{
				Name:        "chat-completion",
				Description: "Chat completion",
			},
		},
	},
}

// TODO: metadata is map[string]string so this needs to be refactored later to be incorporated
// var bindingsServiceBusQueues = metadataschema.ComponentMetadata{
// 	Type:         binding,
// 	Name:         nameServiceBusQueues,
// 	Version:      v1,
// 	Status:       stable,
// 	Title:        titleServiceBusQueues,
// 	Description:  "", // TODO
// 	Capabilities: []string{},
// }

var bindingsStorageQueues = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameStorageQueues,
	Version:      v1,
	Status:       stable,
	Title:        titleStorageQueues,
	Description:  "", // TODO
	Capabilities: []string{},
	AuthenticationProfiles: []metadataschema.AuthenticationProfile{
		{
			Title:       "Account Key",
			Description: "Authenticate using a pre-shared account key",
		},
	},
	BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{azureAuthProfile},
	Binding: &metadataschema.Binding{
		Input:  true,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Publish a new message in the queue.",
			},
		},
	},
}

// TODO(@Sam): here and below
var bindingsSignalR = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameSignalR,
	Version:      v1,
	Status:       alpha,
	Title:        titleSignalR,
	Description:  "", // TODO
	Capabilities: []string{},
}

var bindingsCron = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameCron,
	Version:      v1,
	Status:       stable,
	Title:        titleCron,
	Description:  "", // TODO
	Capabilities: []string{},
}

var bindingsGCPBucket = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameGCPBucket,
	Version:      v1,
	Status:       alpha,
	Title:        titleGCPBucket,
	Description:  "", // TODO
	Capabilities: []string{},
}

var bindingsHTTP = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameHTTP,
	Version:      v1,
	Status:       stable,
	Title:        titleHTTP,
	Description:  "", // TODO
	Capabilities: []string{},
}

var bindingsKafka = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameKafka,
	Version:      v1,
	Status:       stable,
	Title:        titleHTTP,
	Description:  "", // TODO
	Capabilities: []string{},
}

var bindingsMySQL = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameMySQL,
	Version:      v1,
	Status:       alpha,
	Title:        titleMySQL,
	Description:  "", // TODO
	Capabilities: []string{},
}

var bindingsPostgres = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         namePostgresql,
	Version:      v1,
	Status:       stable,
	Title:        titlePostgresql,
	Description:  "",         // TODO
	Capabilities: []string{}, // explicit bc it's diff than other pg components
}

var bindingsRabbit = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameRabbit,
	Version:      v1,
	Status:       stable,
	Title:        titleRabbit,
	Description:  "",         // TODO
	Capabilities: []string{}, // explicit bc it's diff than other pg components
}

var bindingsRedis = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameRedis,
	Version:      v1,
	Status:       stable,
	Title:        titleRedis,
	Description:  "",         // TODO
	Capabilities: []string{}, // explicit bc it's diff than other pg components
}

var bindingsZeebeCommand = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameZeebeCommand,
	Version:      v1,
	Status:       stable,
	Title:        titleZeebeCommand,
	Description:  "",         // TODO
	Capabilities: []string{}, // explicit bc it's diff than other pg components
}

var bindingsZeebeWorker = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameZeebeWorker,
	Version:      v1,
	Status:       stable,
	Title:        titleZeebeWorker,
	Description:  "",         // TODO
	Capabilities: []string{}, // explicit bc it's diff than other pg components
}

var bindingsAppConfig = metadataschema.ComponentMetadata{
	Type:         binding,
	Name:         nameAppConfig,
	Version:      v1,
	Status:       alpha,
	Title:        titleAppConfig,
	Description:  "",         // TODO
	Capabilities: []string{}, // explicit bc it's diff than other pg components
}

var bindingsKinesis = metadataschema.ComponentMetadata{
	Type:                          binding,
	Name:                          nameKinesis,
	Version:                       v1,
	Status:                        alpha,
	Title:                         titleKinesis,
	Description:                   "", // TODO
	BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{awsAuthProfile},
	Binding: &metadataschema.Binding{
		Input:  true,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create a stream",
			},
		},
	},
}

var bindingsDynamo = metadataschema.ComponentMetadata{
	Type:                          binding,
	Name:                          nameDynamo,
	Version:                       v1,
	Status:                        stable,
	Title:                         titleDynamo,
	Description:                   "", // TODO
	Capabilities:                  []string{capabilitiesActorState, capabilitiesCrud, capabilitiesTransaction, capabilitiesEtag, capabilitiesTtl},
	BuiltInAuthenticationProfiles: []metadataschema.BuiltinAuthenticationProfile{awsAuthProfile},
	Binding: &metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create a table",
			},
		},
	},
}
