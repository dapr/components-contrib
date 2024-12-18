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

func GetAllSecretStores() []*metadataschema.ComponentMetadata {
	return []*metadataschema.ComponentMetadata{
		&secretParameterStore,
		&secretManager,
		&secretAzureVault,
		&secretGCP,
		&secretHashiVault,
		&secretK8s,
		&secretLocal,
	}
}

var secretParameterStore = metadataschema.ComponentMetadata{
	Type:         secretstores,
	Name:         nameParameterStore,
	Version:      v1,
	Status:       alpha,
	Title:        titleParameterStore,
	Description:  "", // TODO
	Capabilities: []string{},
}

var secretManager = metadataschema.ComponentMetadata{
	Type:         secretstores,
	Name:         nameSecretManager,
	Version:      v1,
	Status:       beta,
	Title:        titleSecretManager,
	Description:  "", // TODO
	Capabilities: []string{},
}

var secretAzureVault = metadataschema.ComponentMetadata{
	Type:         secretstores,
	Name:         nameAzureVault,
	Version:      v1,
	Status:       stable,
	Title:        titleAzureVault,
	Description:  "", // TODO
	Capabilities: []string{},
}

var secretGCP = metadataschema.ComponentMetadata{
	Type:         secretstores,
	Name:         nameGCPSecret,
	Version:      v1,
	Status:       stable,
	Title:        titleGCPSecret,
	Description:  "", // TODO
	Capabilities: []string{},
}

var secretHashiVault = metadataschema.ComponentMetadata{
	Type:         secretstores,
	Name:         nameHashiVault,
	Version:      v1,
	Status:       stable,
	Title:        titleHashiVault,
	Description:  "", // TODO
	Capabilities: []string{},
}

var secretK8s = metadataschema.ComponentMetadata{
	Type:         secretstores,
	Name:         nameK8s,
	Version:      v1,
	Status:       stable,
	Title:        titleK8s,
	Description:  "", // TODO
	Capabilities: []string{},
}

var secretLocal = metadataschema.ComponentMetadata{
	Type:         secretstores,
	Name:         nameLocal,
	Version:      v1,
	Status:       stable,
	Title:        titleLocal,
	Description:  "", // TODO
	Capabilities: []string{},
}
