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
	"unicode"

	mssql "github.com/denisenkom/go-mssqldb"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
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
func NewSQLServerStateStore(logger logger.Logger) state.Store {
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

type sqlServerMetadata struct {
	ConnectionString  string
	DatabaseName      string
	TableName         string
	Schema            string
	KeyType           string
	KeyLength         int
	IndexedProperties string
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
	err := s.parseMetadata(metadata.Properties)
	if err != nil {
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

func (s *SQLServer) parseMetadata(meta map[string]string) error {
	m := sqlServerMetadata{
		TableName:    defaultTable,
		Schema:       defaultSchema,
		DatabaseName: defaultDatabase,
		KeyLength:    defaultKeyLength,
	}
	err := metadata.DecodeMetadata(meta, &m)
	if err != nil {
		return err
	}
	if m.ConnectionString == "" {
		return fmt.Errorf("missing connection string")
	}
	s.connectionString = m.ConnectionString

	if err := s.setTable(m.TableName); err != nil {
		return err
	}

	if err := s.setDatabase(m.DatabaseName); err != nil {
		return err
	}

	if err := s.setKeyType(m.KeyType, m.KeyLength); err != nil {
		return err
	}

	if err := s.setSchema(m.Schema); err != nil {
		return err
	}

	if err := s.setIndexedProperties(m.IndexedProperties); err != nil {
		return err
	}

	return nil
}

// Returns validated index properties.
func (s *SQLServer) setIndexedProperties(indexedPropertiesString string) error {
	if indexedPropertiesString != "" {
		var indexedProperties []IndexedProperty
		err := json.Unmarshal([]byte(indexedPropertiesString), &indexedProperties)
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
func (s *SQLServer) setKeyType(keyType string, keyLength int) error {
	if keyType != "" {
		kt, err := KeyTypeFromString(keyType)
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

	if keyLength <= 0 {
		return fmt.Errorf("invalid key length value of %d", keyLength)
	} else {
		s.keyLength = keyLength
	}

	return nil
}

// Returns the schema name if set or the default value otherwise.
func (s *SQLServer) setSchema(schemaName string) error {
	if !isValidSQLName(schemaName) {
		return fmt.Errorf("invalid schema name, accepted characters are (A-Z, a-z, 0-9, _)")
	}
	s.schema = schemaName

	return nil
}

// Returns the database name if set or the default value otherwise.
func (s *SQLServer) setDatabase(databaseName string) error {
	if !isValidSQLName(databaseName) {
		return fmt.Errorf("invalid database name, accepted characters are (A-Z, a-z, 0-9, _)")
	}

	s.databaseName = databaseName

	return nil
}

// Returns the table name if set or the default value otherwise.
func (s *SQLServer) setTable(tableName string) error {
	if !isValidSQLName(tableName) {
		return fmt.Errorf("invalid table name, accepted characters are (A-Z, a-z, 0-9, _)")
	}

	s.tableName = tableName

	return nil
}

// Features returns the features available in this state store.
func (s *SQLServer) Features() []state.Feature {
	return s.features
}

// Multi performs multiple updates on a Sql server store.
func (s *SQLServer) Multi(request *state.TransactionalStateRequest) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	for _, req := range request.Operations {
		switch req.Operation {
		case state.Upsert:
			setReq, err := s.getSets(req)
			if err != nil {
				tx.Rollback()
				return err
			}

			err = s.executeSet(tx, &setReq)
			if err != nil {
				tx.Rollback()
				return err
			}

		case state.Delete:
			delReq, err := s.getDeletes(req)
			if err != nil {
				tx.Rollback()
				return err
			}

			err = s.executeDelete(tx, &delReq)
			if err != nil {
				tx.Rollback()
				return err
			}

		default:
			tx.Rollback()
			return fmt.Errorf("unsupported operation: %s", req.Operation)
		}
	}

	return tx.Commit()
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

// Delete removes an entity from the store.
func (s *SQLServer) Delete(req *state.DeleteRequest) error {
	return s.executeDelete(s.db, req)
}

func (s *SQLServer) executeDelete(db dbExecutor, req *state.DeleteRequest) error {
	var err error
	var res sql.Result
	if req.ETag != nil {
		var b []byte
		b, err = hex.DecodeString(*req.ETag)
		if err != nil {
			return state.NewETagError(state.ETagInvalid, err)
		}

		res, err = db.Exec(s.deleteWithETagCommand, sql.Named(keyColumnName, req.Key), sql.Named(rowVersionColumnName, b))
	} else {
		res, err = db.Exec(s.deleteWithoutETagCommand, sql.Named(keyColumnName, req.Key))
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
		err = state.NewBulkDeleteRowMismatchError(uint64(rows), uint64(len(req)))

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
		ETag: ptr.Of(etag),
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

func (s *SQLServer) GetComponentMetadata() map[string]string {
	return map[string]string{}
}
