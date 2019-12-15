package sqlserver

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/dapr/components-contrib/state"
	mssql "github.com/denisenkom/go-mssqldb"
)

// KeyType defines type of the table identifier
type KeyType string

// KeyTypeFromString tries to create a KeyType from a string value
func KeyTypeFromString(k string) (KeyType, error) {
	switch k {
	case string(StringKeyType):
		return StringKeyType, nil
	case string(UUIDKeyType):
		return UUIDKeyType, nil
	case string(IntegerKeyType):
		return IntegerKeyType, nil
	}

	return InvalidKeyType, errors.New("Invalid key type")
}

const (
	// StringKeyType defines a key of type string
	StringKeyType KeyType = "string"

	// UUIDKeyType defines a key of type UUID/GUID
	UUIDKeyType KeyType = "uuid"

	//IntegerKeyType defines a key of type integer
	IntegerKeyType KeyType = "integer"

	//InvalidKeyType defines an invalid key type
	InvalidKeyType KeyType = "invalid"
)

const (
	connectionStringKey  = "connectionString"
	tableNameKey         = "tableName"
	schemaKey            = "schema"
	keyTypeKey           = "keyType"
	keyLengthKey         = "keyLength"
	indexedPropertiesKey = "indexedProperties"
	keyColumnName        = "Key"
	rowVersionColumnName = "RowVersion"

	defaultMaxKeyLength = 200
	defaultSchema       = "dbo"
)

// NewSQLServerStore creates a new instance of a Sql Server transaction store
func NewSQLServerStore() *StateStore {
	return &StateStore{}
}

// IndexedProperty defines a indexed property
type IndexedProperty struct {
	ColumnName string `json:"column"`
	Property   string `json:"property"`
	Type       string `json:"type"`
}

// StateStore defines a Sql Server based state store
type StateStore struct {
	connectionString  string
	tableName         string
	schema            string
	keyType           KeyType
	keyLength         int
	indexedProperties []IndexedProperty

	bulkDeleteCommand        string
	itemRefTableTypeName     string
	upsertCommand            string
	getCommand               string
	deleteWithETagCommand    string
	deleteWithoutETagCommand string
}

// Init initializes the SQL server state store
func (s *StateStore) Init(metadata state.Metadata) error {

	if val, ok := metadata.Properties[connectionStringKey]; ok && val != "" {
		s.connectionString = val
	} else {
		return fmt.Errorf("missing connection string")
	}

	if val, ok := metadata.Properties[tableNameKey]; ok && val != "" {
		s.tableName = val
	} else {
		return fmt.Errorf("missing table name")
	}

	if val, ok := metadata.Properties[keyTypeKey]; ok && val != "" {
		kt, err := KeyTypeFromString(val)
		if err != nil {
			return err
		}
		s.keyType = kt
	} else {
		s.keyType = StringKeyType
	}

	if s.keyType == StringKeyType {
		if val, ok := metadata.Properties[keyLengthKey]; ok && val != "" {
			var err error
			s.keyLength, err = strconv.Atoi(val)
			if err != nil {
				return err
			}

			if s.keyLength <= 0 {
				return fmt.Errorf("invalid key length value of %d", s.keyLength)
			}
		} else {
			s.keyLength = defaultMaxKeyLength
		}
	}

	if val, ok := metadata.Properties[schemaKey]; ok && val != "" {
		s.schema = val
	} else {
		s.schema = defaultSchema
	}

	if val, ok := metadata.Properties[indexedPropertiesKey]; ok && val != "" {
		var indexedProperties []IndexedProperty
		err := json.Unmarshal([]byte(val), &indexedProperties)
		if err != nil {
			return err
		}

		s.indexedProperties = indexedProperties
	}

	migration := newMigration(s)
	mr, err := migration.ensureDatabaseExists()
	if err != nil {
		return err
	}

	s.itemRefTableTypeName = mr.itemRefTableTypeName
	s.bulkDeleteCommand = fmt.Sprintf("exec %s @itemsToDelete;", mr.bulkDeleteProcFullName)
	s.upsertCommand = mr.upsertProcFullName
	s.getCommand = fmt.Sprintf("SELECT [Data], [RowVersion] FROM [%s].[%s] WHERE [Key] = @Key", s.schema, s.tableName)
	s.deleteWithETagCommand = fmt.Sprintf(`DELETE [%s].[%s] WHERE [Key]=@Key AND [RowVersion]=@RowVersion`, s.schema, s.tableName)
	s.deleteWithoutETagCommand = fmt.Sprintf(`DELETE [%s].[%s] WHERE [Key]=@Key`, s.schema, s.tableName)

	return nil
}

// Multi performs multiple updates on a Sql server store
func (s *StateStore) Multi(reqs []state.TransactionalRequest) error {

	var deletes []state.DeleteRequest
	var sets []state.SetRequest
	for _, req := range reqs {
		switch req.Operation {
		case state.Upsert:
			setReq, ok := req.Request.(state.SetRequest)
			if !ok {
				return fmt.Errorf("expecting set request")
			}

			if setReq.Key == "" {
				return fmt.Errorf("missing key in upsert operation")
			}

			sets = append(sets, setReq)
			break
		case state.Delete:

			delReq, ok := req.Request.(state.DeleteRequest)
			if !ok {
				return fmt.Errorf("expecting delete request")
			}

			if delReq.Key == "" {
				return fmt.Errorf("missing key in upsert operation")
			}

			deletes = append(deletes, delReq)
			break
		default:
			return fmt.Errorf("unsupported operation: %s", req.Operation)
		}
	}

	if len(sets) > 0 || len(deletes) > 0 {
		return s.executeMulti(sets, deletes)
	}

	return nil
}

func (s *StateStore) executeMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error {
	db, err := sql.Open("sqlserver", s.connectionString)
	if err != nil {
		return err
	}

	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	if len(deletes) > 0 {
		err = s.executeBulkDelete(tx, deletes)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	if len(sets) > 0 {
		for _, set := range sets {
			err = s.executeSet(tx, &set)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit()
}

// Delete removes an entity from the store
func (s *StateStore) Delete(req *state.DeleteRequest) error {

	db, err := sql.Open("sqlserver", s.connectionString)
	if err != nil {
		return err
	}

	defer db.Close()

	var res sql.Result
	if req.ETag != "" {
		b, err := hexStringToBytes(req.ETag)
		if err != nil {
			return err
		}

		res, err = db.Exec(s.deleteWithETagCommand, sql.Named(keyColumnName, req.Key), sql.Named(rowVersionColumnName, b))

	} else {
		res, err = db.Exec(s.deleteWithoutETagCommand, sql.Named(keyColumnName, req.Key))
	}

	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 {
		return fmt.Errorf("items was not updated")
	}

	return nil
}

// TvpDeleteTableStringKey defines a table type with string key
type TvpDeleteTableStringKey struct {
	ID         string
	RowVersion []byte
}

// BulkDelete removes multiple entries from the store
func (s *StateStore) BulkDelete(req []state.DeleteRequest) error {

	db, err := sql.Open("sqlserver", s.connectionString)
	if err != nil {
		return err
	}

	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	err = s.executeBulkDelete(tx, req)
	if err != nil {
		tx.Rollback()
		return err
	}

	tx.Commit()

	return nil
}

func (s *StateStore) executeBulkDelete(db dbExecutor, req []state.DeleteRequest) error {
	values := make([]TvpDeleteTableStringKey, len(req))
	for i, d := range req {
		var etag []byte
		var err error
		if d.ETag != "" {
			etag, err = hexStringToBytes(d.ETag)
			if err != nil {
				return err
			}
		}
		values[i] = TvpDeleteTableStringKey{ID: d.Key, RowVersion: etag}
	}

	itemsToDelete := mssql.TVP{
		TypeName: s.itemRefTableTypeName,
		Value:    values,
	}

	res, err := db.Exec(s.bulkDeleteCommand, sql.Named("itemsToDelete", itemsToDelete))
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if int(rows) != len(req) {
		err = fmt.Errorf("delete affected only %d rows, expected %d", rows, len(req))
		return err
	}

	return nil
}

// Get returns an entity from store
func (s *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {

	db, err := sql.Open("sqlserver", s.connectionString)
	if err != nil {
		return nil, err
	}

	defer db.Close()

	rows, err := db.Query(s.getCommand, sql.Named(keyColumnName, req.Key))

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	if !rows.Next() {
		return nil, errors.New("not found")
	}

	var data string
	var rowVersion []byte
	err = rows.Scan(&data, &rowVersion)
	if err != nil {
		return nil, err
	}

	etag := hex.EncodeToString(rowVersion)

	return &state.GetResponse{
		Data: []byte(data),
		ETag: etag,
	}, nil
}

// Set adds/updates an entity on store
func (s *StateStore) Set(req *state.SetRequest) error {

	db, err := sql.Open("sqlserver", s.connectionString)
	if err != nil {
		return err
	}

	defer db.Close()

	return s.executeSet(db, req)
}

// dbExecutor implements a common functionality implemented by db or tx
type dbExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func (s *StateStore) executeSet(db dbExecutor, req *state.SetRequest) error {
	json, err := json.Marshal(req.Value)
	if err != nil {
		return err
	}

	etag := sql.Named(rowVersionColumnName, nil)
	if req.ETag != "" {
		b, err := hexStringToBytes(req.ETag)
		if err != nil {
			return err
		}
		etag.Value = b
	}
	res, err := db.Exec(s.upsertCommand, sql.Named(keyColumnName, req.Key), sql.Named("Data", string(json)), etag)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 {
		return fmt.Errorf("no item was updated")
	}

	return nil
}

// BulkSet adds/updates multiple entities on store
func (s *StateStore) BulkSet(req []state.SetRequest) error {
	db, err := sql.Open("sqlserver", s.connectionString)
	if err != nil {
		return err
	}

	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, r := range req {
		err = s.executeSet(tx, &r)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	return err
}
