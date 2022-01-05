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

package sqlserver

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"unicode"

	"github.com/agrea/ptr"
	mssql "github.com/denisenkom/go-mssqldb"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

// KeyType defines type of the table identifier.
type KeyType string

// KeyTypeFromString tries to create a KeyType from a string value.
func KeyTypeFromString(k string) (KeyType, error) {
	switch k {
	case string(StringKeyType):
		return StringKeyType, nil
	case string(UUIDKeyType):
		return UUIDKeyType, nil
	case string(IntegerKeyType):
		return IntegerKeyType, nil
	}

	return InvalidKeyType, errors.New("invalid key type")
}

const (
	// StringKeyType defines a key of type string.
	StringKeyType KeyType = "string"

	// UUIDKeyType defines a key of type UUID/GUID.
	UUIDKeyType KeyType = "uuid"

	// IntegerKeyType defines a key of type integer.
	IntegerKeyType KeyType = "integer"

	// InvalidKeyType defines an invalid key type.
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
	databaseNameKey      = "databaseName"

	defaultKeyLength = 200
	defaultSchema    = "dbo"
	defaultDatabase  = "dapr"
	defaultTable     = "state"
)

// NewSQLServerStateStore creates a new instance of a Sql Server transaction store.
func NewSQLServerStateStore(logger logger.Logger) *SQLServer {
	store := SQLServer{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
		logger:   logger,
	}
	store.migratorFactory = newMigration

	return &store
}

// IndexedProperty defines a indexed property.
type IndexedProperty struct {
	ColumnName string `json:"column"`
	Property   string `json:"property"`
	Type       string `json:"type"`
}

// SQLServer defines a Ms SQL Server based state store.
type SQLServer struct {
	connectionString  string
	databaseName      string
	tableName         string
	schema            string
	keyType           KeyType
	keyLength         int
	indexedProperties []IndexedProperty
	migratorFactory   func(*SQLServer) migrator

	bulkDeleteCommand        string
	itemRefTableTypeName     string
	upsertCommand            string
	getCommand               string
	deleteWithETagCommand    string
	deleteWithoutETagCommand string

	features []state.Feature
	logger   logger.Logger
	db       *sql.DB
}

func isLetterOrNumber(c rune) bool {
	return unicode.IsNumber(c) || unicode.IsLetter(c)
}

func isValidSQLName(s string) bool {
	for _, c := range s {
		if !(isLetterOrNumber(c) || (c == '_')) {
			return false
		}
	}

	return true
}

func isValidIndexedPropertyName(s string) bool {
	for _, c := range s {
		if !(isLetterOrNumber(c) || (c == '_') || (c == '.') || (c == '[') || (c == ']')) {
			return false
		}
	}

	return true
}

func isValidIndexedPropertyType(s string) bool {
	for _, c := range s {
		if !(isLetterOrNumber(c) || (c == '(') || (c == ')')) {
			return false
		}
	}

	return true
}

// Init initializes the SQL server state store.
func (s *SQLServer) Init(metadata state.Metadata) error {
	if val, ok := metadata.Properties[connectionStringKey]; ok && val != "" {
		s.connectionString = val
	} else {
		return fmt.Errorf("missing connection string")
	}

	if err := s.getTable(metadata); err != nil {
		return err
	}

	if err := s.getDatabase(metadata); err != nil {
		return err
	}

	if err := s.getKeyType(metadata); err != nil {
		return err
	}

	if err := s.getSchema(metadata); err != nil {
		return err
	}

	if err := s.getIndexedProperties(metadata); err != nil {
		return err
	}

	migration := s.migratorFactory(s)
	mr, err := migration.executeMigrations()
	if err != nil {
		return err
	}

	s.itemRefTableTypeName = mr.itemRefTableTypeName
	s.bulkDeleteCommand = fmt.Sprintf("exec %s @itemsToDelete;", mr.bulkDeleteProcFullName)
	s.upsertCommand = mr.upsertProcFullName
	s.getCommand = mr.getCommand
	s.deleteWithETagCommand = mr.deleteWithETagCommand
	s.deleteWithoutETagCommand = mr.deleteWithoutETagCommand

	s.db, err = sql.Open("sqlserver", s.connectionString)
	if err != nil {
		return err
	}

	return nil
}

// Returns validated index properties.
func (s *SQLServer) getIndexedProperties(metadata state.Metadata) error {
	if val, ok := metadata.Properties[indexedPropertiesKey]; ok && val != "" {
		var indexedProperties []IndexedProperty
		err := json.Unmarshal([]byte(val), &indexedProperties)
		if err != nil {
			return err
		}

		err = s.validateIndexedProperties(indexedProperties)
		if err != nil {
			return err
		}

		s.indexedProperties = indexedProperties
	}

	return nil
}

// Validates that all the mandator index properties are supplied and that the
// values are valid.
func (s *SQLServer) validateIndexedProperties(indexedProperties []IndexedProperty) error {
	for _, p := range indexedProperties {
		if p.ColumnName == "" {
			return errors.New("indexed property column cannot be empty")
		}

		if p.Property == "" {
			return errors.New("indexed property name cannot be empty")
		}

		if p.Type == "" {
			return errors.New("indexed property type cannot be empty")
		}

		if !isValidSQLName(p.ColumnName) {
			return fmt.Errorf("invalid indexed property column name, accepted characters are (A-Z, a-z, 0-9, _)")
		}

		if !isValidIndexedPropertyName(p.Property) {
			return fmt.Errorf("invalid indexed property name, accepted characters are (A-Z, a-z, 0-9, _, ., [, ])")
		}

		if !isValidIndexedPropertyType(p.Type) {
			return fmt.Errorf("invalid indexed property type, accepted characters are (A-Z, a-z, 0-9, _, (, ))")
		}
	}

	return nil
}

// Validates and returns the key type.
func (s *SQLServer) getKeyType(metadata state.Metadata) error {
	if val, ok := metadata.Properties[keyTypeKey]; ok && val != "" {
		kt, err := KeyTypeFromString(val)
		if err != nil {
			return err
		}

		s.keyType = kt
	} else {
		s.keyType = StringKeyType
	}

	if s.keyType != StringKeyType {
		return nil
	}

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
		s.keyLength = defaultKeyLength
	}

	return nil
}

// Returns the schema name if set or the default value otherwise.
func (s *SQLServer) getSchema(metadata state.Metadata) error {
	if val, ok := metadata.Properties[schemaKey]; ok && val != "" {
		if !isValidSQLName(val) {
			return fmt.Errorf("invalid schema name, accepted characters are (A-Z, a-z, 0-9, _)")
		}
		s.schema = val
	} else {
		s.schema = defaultSchema
	}

	return nil
}

// Returns the database name if set or the default value otherwise.
func (s *SQLServer) getDatabase(metadata state.Metadata) error {
	if val, ok := metadata.Properties[databaseNameKey]; ok && val != "" {
		if !isValidSQLName(val) {
			return fmt.Errorf("invalid database name, accepted characters are (A-Z, a-z, 0-9, _)")
		}

		s.databaseName = val
	} else {
		s.databaseName = defaultDatabase
	}

	return nil
}

// Returns the table name if set or the default value otherwise.
func (s *SQLServer) getTable(metadata state.Metadata) error {
	if val, ok := metadata.Properties[tableNameKey]; ok && val != "" {
		if !isValidSQLName(val) {
			return fmt.Errorf("invalid table name, accepted characters are (A-Z, a-z, 0-9, _)")
		}

		s.tableName = val
	} else {
		s.tableName = defaultTable
	}

	return nil
}

func (s *SQLServer) Ping() error {
	return nil
}

// Features returns the features available in this state store.
func (s *SQLServer) Features() []state.Feature {
	return s.features
}

// Multi performs multiple updates on a Sql server store.
func (s *SQLServer) Multi(request *state.TransactionalStateRequest) error {
	var sets []state.SetRequest
	var deletes []state.DeleteRequest
	for _, req := range request.Operations {
		switch req.Operation {
		case state.Upsert:
			setReq, err := s.getSets(req)
			if err != nil {
				return err
			}

			sets = append(sets, setReq)

		case state.Delete:
			delReq, err := s.getDeletes(req)
			if err != nil {
				return err
			}

			deletes = append(deletes, delReq)

		default:
			return fmt.Errorf("unsupported operation: %s", req.Operation)
		}
	}

	if len(sets) > 0 || len(deletes) > 0 {
		return s.executeMulti(sets, deletes)
	}

	return nil
}

// Returns the set requests.
func (s *SQLServer) getSets(req state.TransactionalStateOperation) (state.SetRequest, error) {
	setReq, ok := req.Request.(state.SetRequest)
	if !ok {
		return setReq, fmt.Errorf("expecting set request")
	}

	if setReq.Key == "" {
		return setReq, fmt.Errorf("missing key in upsert operation")
	}

	return setReq, nil
}

// Returns the delete requests.
func (s *SQLServer) getDeletes(req state.TransactionalStateOperation) (state.DeleteRequest, error) {
	delReq, ok := req.Request.(state.DeleteRequest)
	if !ok {
		return delReq, fmt.Errorf("expecting delete request")
	}

	if delReq.Key == "" {
		return delReq, fmt.Errorf("missing key in upsert operation")
	}

	return delReq, nil
}

func (s *SQLServer) executeMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error {
	tx, err := s.db.Begin()
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
		for i := range sets {
			err = s.executeSet(tx, &sets[i])
			if err != nil {
				tx.Rollback()

				return err
			}
		}
	}

	return tx.Commit()
}

// Delete removes an entity from the store.
func (s *SQLServer) Delete(req *state.DeleteRequest) error {
	var err error
	var res sql.Result
	if req.ETag != nil {
		var b []byte
		b, err = hex.DecodeString(*req.ETag)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}

		res, err = s.db.Exec(s.deleteWithETagCommand, sql.Named(keyColumnName, req.Key), sql.Named(rowVersionColumnName, b))
	} else {
		res, err = s.db.Exec(s.deleteWithoutETagCommand, sql.Named(keyColumnName, req.Key))
	}

	// err represents errors thrown by the stored procedure or the database itself
	if err != nil {
		return err
	}

	// if the row with matching key (and ETag if specified) is not found, then the stored procedure returns 0 rows affected
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	// When an ETAG is specified, a row must have been deleted or else we return an ETag mismatch error
	if rows != 1 && req.ETag != nil && *req.ETag != "" {
		return state.NewETagError(state.ETagMismatch, nil)
	}

	// successful deletion, or noop if no ETAG specified
	return nil
}

// TvpDeleteTableStringKey defines a table type with string key.
type TvpDeleteTableStringKey struct {
	ID         string
	RowVersion []byte
}

// BulkDelete removes multiple entries from the store.
func (s *SQLServer) BulkDelete(req []state.DeleteRequest) error {
	tx, err := s.db.Begin()
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

func (s *SQLServer) executeBulkDelete(db dbExecutor, req []state.DeleteRequest) error {
	values := make([]TvpDeleteTableStringKey, len(req))
	for i, d := range req {
		var etag []byte
		var err error
		if d.ETag != nil {
			etag, err = hex.DecodeString(*d.ETag)
			if err != nil {
				return state.NewETagError(state.ETagInvalid, err)
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

// Get returns an entity from store.
func (s *SQLServer) Get(req *state.GetRequest) (*state.GetResponse, error) {
	rows, err := s.db.Query(s.getCommand, sql.Named(keyColumnName, req.Key))
	if err != nil {
		return nil, err
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	defer rows.Close()

	if !rows.Next() {
		return &state.GetResponse{}, nil
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
		ETag: ptr.String(etag),
	}, nil
}

// BulkGet performs a bulks get operations.
func (s *SQLServer) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	return false, nil, nil
}

// Set adds/updates an entity on store.
func (s *SQLServer) Set(req *state.SetRequest) error {
	return s.executeSet(s.db, req)
}

// dbExecutor implements a common functionality implemented by db or tx.
type dbExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func (s *SQLServer) executeSet(db dbExecutor, req *state.SetRequest) error {
	var err error
	var bytes []byte
	bytes, err = utils.Marshal(req.Value, json.Marshal)
	if err != nil {
		return err
	}
	etag := sql.Named(rowVersionColumnName, nil)
	if req.ETag != nil && *req.ETag != "" {
		var b []byte
		b, err = hex.DecodeString(*req.ETag)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}
		etag = sql.Named(rowVersionColumnName, b)
	}

	var res sql.Result
	if req.Options.Concurrency == state.FirstWrite {
		res, err = db.Exec(s.upsertCommand, sql.Named(keyColumnName, req.Key), sql.Named("Data", string(bytes)), etag, sql.Named("FirstWrite", 1))
	} else {
		res, err = db.Exec(s.upsertCommand, sql.Named(keyColumnName, req.Key), sql.Named("Data", string(bytes)), etag, sql.Named("FirstWrite", 0))
	}

	if err != nil {
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}

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

// BulkSet adds/updates multiple entities on store.
func (s *SQLServer) BulkSet(req []state.SetRequest) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	for i := range req {
		err = s.executeSet(tx, &req[i])
		if err != nil {
			tx.Rollback()

			return err
		}
	}

	err = tx.Commit()

	return err
}
