package tablestorage

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/dapr/components-contrib/state"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"strings"
)

const (
	fullmetadata        = "application/json;odata=fullmetadata"
	daprDelim           = "__delim__"
	valueEntityProperty = "Value"

	accountNameKey = "accountName"
	accountKeyKey  = "accountKey"
	tableNameKey   = "tableName"
)

type StateStore struct {
	table *storage.Table
	json  jsoniter.API
}

type tablesMetadata struct {
	accountName string
	accountKey  string
	tableName   string
}

// ---- Store interface begin

//initialises connection to table storage, optionally creates a table if it doesn't exist
func (r *StateStore) Init(metadata state.Metadata) error {
	meta, err := getTablesMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	client, _ := storage.NewBasicClient(meta.accountName, meta.accountKey)
	tables := client.GetTableService()
	r.table = tables.GetTableReference(meta.tableName)

	//check table exists
	log.Debugf("using table '%s'", meta.tableName)
	err = r.table.Create(100, fullmetadata, nil)
	if err != nil {
		if azureError, ok := err.(storage.AzureStorageServiceError); ok && azureError.Code == "TableAlreadyExists" {
			//error creating table, but it already exists so we're fine
			log.Debugf("table already exists")
		} else {
			return err
		}
	}

	log.Printf("table initialised, account: %s, table: %s", meta.accountName, meta.tableName)

	return nil
}

func (r *StateStore) Delete(req *state.DeleteRequest) error {
	log.Debugf("delete one")
	return nil
}

func (r *StateStore) BulkDelete(req []state.DeleteRequest) error {
	log.Debugf("bulk delete")
	return nil
}

func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	log.Debugf("GET ONE")
	pk, rk := r.getPartitionAndRowKey(req.Key)
	entity := r.table.GetEntityReference(pk, rk)
	err := entity.Get(1000, fullmetadata, nil)

	if err != nil {
		if azureError, ok := err.(storage.AzureStorageServiceError); ok && azureError.Code == "ResourceNotFound" {
			return &state.GetResponse{}, nil
		}

		return &state.GetResponse{}, err
	}

	data, err := r.unmarshal(entity)
	return &state.GetResponse{
		Data: data,
	}, err
}

func (r *StateStore) Set(req *state.SetRequest) error {
	log.Debugf("set ONE")

	return r.writeRow(req)
}

func (r *StateStore) BulkSet(req []state.SetRequest) error {
	log.Debugf("bulk set %v", len(req))

	for _, s := range req {
		err := r.Set(&s)
		if err != nil {
			return err
		}
	}

	return nil
}

// ---- Store interface end

func NewAzureTablesStateStore() *StateStore {
	return &StateStore{
		json: jsoniter.ConfigFastest,
	}
}

func getTablesMetadata(metadata map[string]string) (*tablesMetadata, error) {
	meta := tablesMetadata{}

	if val, ok := metadata[accountNameKey]; ok && val != "" {
		meta.accountName = val
	} else {
		return nil, errors.New(fmt.Sprintf("missing or empty %s field from metadata", accountNameKey))
	}

	if val, ok := metadata[accountKeyKey]; ok && val != "" {
		meta.accountKey = val
	} else {
		return nil, errors.New(fmt.Sprintf("missing of empty %s field from metadata", accountKeyKey))
	}

	if val, ok := metadata[tableNameKey]; ok && val != "" {
		meta.tableName = val
	} else {
		return nil, errors.New(fmt.Sprintf("missing of empty %s field from metadata", tableNameKey))
	}

	return &meta, nil
}

func (r *StateStore) writeRow(req *state.SetRequest) error {
	pk, rk := r.getPartitionAndRowKey(req.Key)
	entity := r.table.GetEntityReference(pk, rk)
	entity.Properties = map[string]interface{}{
		valueEntityProperty: r.marshal(req),
		"ETag":              req.ETag, //todo
	}

	return entity.InsertOrReplace(nil)
}

func (r *StateStore) getPartitionAndRowKey(key string) (string, string) {
	pr := strings.Split(key, daprDelim)
	return pr[0], pr[1]
}

func (r *StateStore) marshal(req *state.SetRequest) string {
	var v string
	b, ok := req.Value.([]byte)
	if ok {
		v = string(b)
	} else {
		v, _ = jsoniter.MarshalToString(req.Value)
	}
	return v
}

func (r *StateStore) unmarshal(row *storage.Entity) ([]byte, error) {
	raw := row.Properties[valueEntityProperty]

	//value column not present
	if raw == nil {
		return nil, nil
	}

	//must be a string
	sv, ok := raw.(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("expected string in column '%s'", valueEntityProperty))
	}

	return []byte(sv), nil
}
