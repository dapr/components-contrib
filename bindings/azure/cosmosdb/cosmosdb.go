// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosdb

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/a8m/documentdb"

	"github.com/dapr/components-contrib/authentication/azure"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// CosmosDB allows performing state operations on collections.
type CosmosDB struct {
	client       *documentdb.DocumentDB
	collection   *documentdb.Collection
	db           *documentdb.Database
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
	client := documentdb.New(m.URL, config)

	dbs, err := client.QueryDatabases(&documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: m.Database},
		},
	})
	if err != nil {
		return err
	} else if len(dbs) == 0 {
		return fmt.Errorf("database %s for CosmosDB state store not found", m.Database)
	}

	c.db = &dbs[0]
	colls, err := client.QueryCollections(c.db.Self, &documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: m.Collection},
		},
	})
	if err != nil {
		return err
	} else if len(colls) == 0 {
		return fmt.Errorf("collection %s for CosmosDB state store not found", m.Collection)
	}

	c.collection = &colls[0]
	c.client = client

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

		_, err = c.client.CreateDocument(c.collection.Self, obj, documentdb.PartitionKey(val))
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
		return nil, fmt.Errorf("missing partitionKey field %s from request body - %s", c.partitionKey, err)
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
