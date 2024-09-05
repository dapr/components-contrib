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

package cosmosdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/common/authentication/azure"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

// CosmosDB allows performing state operations on collections.
type CosmosDB struct {
	client       *azcosmos.ContainerClient
	partitionKey string

	logger logger.Logger
}

type cosmosDBCredentials struct {
	URL          string `json:"url"`
	MasterKey    string `json:"masterKey"`
	Database     string `json:"database"`
	Collection   string `json:"collection"`
	PartitionKey string `json:"partitionKey"`
}

// Value used for timeout durations
const timeoutValue = 30

// NewCosmosDB returns a new CosmosDB instance.
func NewCosmosDB(logger logger.Logger) bindings.OutputBinding {
	return &CosmosDB{logger: logger}
}

// Init performs CosmosDB connection parsing and connecting.
func (c *CosmosDB) Init(ctx context.Context, metadata bindings.Metadata) error {
	m, err := c.parseMetadata(metadata)
	if err != nil {
		return err
	}

	c.partitionKey = m.PartitionKey

	opts := azcosmos.ClientOptions{
		ClientOptions: policy.ClientOptions{
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "dapr-" + logger.DaprVersion,
			},
		},
	}

	// Create the client; first, try authenticating with a master key, if present
	var client *azcosmos.Client
	if m.MasterKey != "" {
		cred, keyErr := azcosmos.NewKeyCredential(m.MasterKey)
		if keyErr != nil {
			return keyErr
		}
		client, err = azcosmos.NewClientWithKey(m.URL, cred, &opts)
		if err != nil {
			return err
		}
	} else {
		// Fallback to using Azure AD
		env, errEnv := azure.NewEnvironmentSettings(metadata.Properties)
		if errEnv != nil {
			return errEnv
		}
		token, errToken := env.GetTokenCredential()
		if errToken != nil {
			return errToken
		}
		client, err = azcosmos.NewClient(m.URL, token, &opts)
		if err != nil {
			return err
		}
	}

	// Create a container client
	dbContainer, err := client.NewContainer(m.Database, m.Collection)
	if err != nil {
		return err
	}

	c.client = dbContainer
	readCtx, readCancel := context.WithTimeout(ctx, timeoutValue*time.Second)
	defer readCancel()
	_, err = c.client.Read(readCtx, nil)
	return err
}

func (c *CosmosDB) parseMetadata(metadata bindings.Metadata) (*cosmosDBCredentials, error) {
	creds := cosmosDBCredentials{}
	err := kitmd.DecodeMetadata(metadata.Properties, &creds)
	if err != nil {
		return nil, err
	}

	return &creds, nil
}

func (c *CosmosDB) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (c *CosmosDB) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		var obj interface{}
		err := json.Unmarshal(req.Data, &obj)
		if err != nil {
			return nil, err
		}

		pkString, err := c.getPartitionKeyValue(c.partitionKey, obj)
		if err != nil {
			return nil, err
		}
		pk := azcosmos.NewPartitionKeyString(pkString)

		_, err = c.client.CreateItem(ctx, pk, req.Data, nil)
		if err != nil {
			return nil, err
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("operation kind %s not supported", req.Operation)
	}
}

func (c *CosmosDB) getPartitionKeyValue(key string, obj interface{}) (string, error) {
	valI, err := c.lookup(obj.(map[string]interface{}), strings.Split(key, "."))
	if err != nil {
		return "", fmt.Errorf("missing partitionKey field %s from request body - %w", c.partitionKey, err)
	}
	val, ok := valI.(string)
	if !ok {
		return "", errors.New("partition key is not a string")
	}

	if val == "" {
		return "", fmt.Errorf("partitionKey field %s from request body is empty", c.partitionKey)
	}

	return val, nil
}

func (c *CosmosDB) lookup(m map[string]interface{}, ks []string) (val interface{}, err error) {
	var ok bool

	if len(ks) == 0 {
		return nil, errors.New("needs at least one key")
	}

	if val, ok = m[ks[0]]; !ok {
		return nil, fmt.Errorf("key not found %v", ks[0])
	}

	// Last Key
	if len(ks) == 1 {
		return val, nil
	}

	// Convert val to map to iterate again
	if m, ok = val.(map[string]interface{}); !ok {
		return nil, fmt.Errorf("invalid structure at %#v", val)
	}

	return c.lookup(m, ks[1:])
}

// GetComponentMetadata returns the metadata of the component.
func (c *CosmosDB) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := cosmosDBCredentials{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}

func (c *CosmosDB) Close() error {
	return nil
}
