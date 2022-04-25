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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/a8m/documentdb"
	"github.com/cenkalti/backoff/v4"

	"github.com/dapr/components-contrib/authentication/azure"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// CosmosDB allows performing state operations on collections.
type CosmosDB struct {
	client       *documentdb.DocumentDB
	collection   string
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

const statusTooManyRequests = "429" // RFC 6585, 4

// NewCosmosDB returns a new CosmosDB instance.
func NewCosmosDB(logger logger.Logger) *CosmosDB {
	return &CosmosDB{logger: logger}
}

// Init performs CosmosDB connection parsing and connecting.
func (c *CosmosDB) Init(metadata bindings.Metadata) error {
	m, err := c.parseMetadata(metadata)
	if err != nil {
		return err
	}

	c.partitionKey = m.PartitionKey

	// Create the client; first, try authenticating with a master key, if present
	var config *documentdb.Config
	if m.MasterKey != "" {
		config = documentdb.NewConfig(&documentdb.Key{
			Key: m.MasterKey,
		})
	} else {
		// Fallback to using Azure AD
		env, errB := azure.NewEnvironmentSettings("cosmosdb", metadata.Properties)
		if errB != nil {
			return errB
		}
		spt, errB := env.GetServicePrincipalToken()
		if errB != nil {
			return errB
		}
		config = documentdb.NewConfigWithServicePrincipal(spt)
	}
	// disable the identification hydrator (which autogenerates IDs if missing from the request)
	// so we aren't forced to use a struct by the upstream SDK
	// this allows us to provide the most flexibility in the request document sent to this binding
	config.IdentificationHydrator = nil
	config.WithAppIdentifier("dapr-" + logger.DaprVersion)

	c.client = documentdb.New(m.URL, config)

	// Retries initializing the client if a TooManyRequests error is encountered
	err = retryOperation(func() (err error) {
		collLink := fmt.Sprintf("dbs/%s/colls/%s/", m.Database, m.Collection)
		coll, err := c.client.ReadCollection(collLink)
		if err != nil {
			if isTooManyRequestsError(err) {
				return err
			}
			return backoff.Permanent(err)
		} else if coll == nil || coll.Self == "" {
			return backoff.Permanent(
				fmt.Errorf("collection %s in database %s for CosmosDB state store not found. This must be created before Dapr uses it", m.Collection, m.Database),
			)
		}

		c.collection = coll.Self

		return nil
	}, func(err error, d time.Duration) {
		c.logger.Warnf("CosmosDB binding initialization failed: %v; retrying in %s", err, d)
	}, 5*time.Minute)
	if err != nil {
		return err
	}

	return nil
}

func (c *CosmosDB) parseMetadata(metadata bindings.Metadata) (*cosmosDBCredentials, error) {
	connInfo := metadata.Properties
	b, err := json.Marshal(connInfo)
	if err != nil {
		return nil, err
	}

	var creds cosmosDBCredentials
	err = json.Unmarshal(b, &creds)
	if err != nil {
		return nil, err
	}

	return &creds, nil
}

func (c *CosmosDB) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (c *CosmosDB) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		var obj interface{}
		err := json.Unmarshal(req.Data, &obj)
		if err != nil {
			return nil, err
		}

		val, err := c.getPartitionKeyValue(c.partitionKey, obj)
		if err != nil {
			return nil, err
		}

		err = retryOperation(func() error {
			_, innerErr := c.client.CreateDocument(c.collection, obj, documentdb.PartitionKey(val))
			if innerErr != nil {
				if isTooManyRequestsError(innerErr) {
					return innerErr
				}
				return backoff.Permanent(innerErr)
			}
			return nil
		}, func(err error, d time.Duration) {
			c.logger.Warnf("CosmosDB binding Invoke request failed: %v; retrying in %s", err, d)
		}, 20*time.Second)
		if err != nil {
			return nil, err
		}

		return nil, nil
	default:
		return nil, fmt.Errorf("operation kind %s not supported", req.Operation)
	}
}

func (c *CosmosDB) getPartitionKeyValue(key string, obj interface{}) (interface{}, error) {
	val, err := c.lookup(obj.(map[string]interface{}), strings.Split(key, "."))
	if err != nil {
		return nil, fmt.Errorf("missing partitionKey field %s from request body - %w", c.partitionKey, err)
	}

	if val == "" {
		return nil, fmt.Errorf("partitionKey field %s from request body is empty", c.partitionKey)
	}

	return val, nil
}

func (c *CosmosDB) lookup(m map[string]interface{}, ks []string) (val interface{}, err error) {
	var ok bool

	if len(ks) == 0 {
		return nil, fmt.Errorf("needs at least one key")
	}

	c.logger.Infof("%s, %s", ks[0], m[ks[0]])

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

func retryOperation(operation backoff.Operation, notify backoff.Notify, maxElapsedTime time.Duration) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 2 * time.Second
	bo.MaxElapsedTime = maxElapsedTime
	return backoff.RetryNotify(operation, bo, notify)
}

func isTooManyRequestsError(err error) bool {
	if err == nil {
		return false
	}

	if requestError, ok := err.(*documentdb.RequestError); ok {
		if requestError.Code == statusTooManyRequests {
			return true
		}
	}

	return false
}
