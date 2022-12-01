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

package mysql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// Optimistic Concurrency is implemented using a string column that stores a UUID.

const (
	// The key name in the metadata if the user wants a different table name
	// than the defaultTableName.
	keyTableName = "tableName"

	// The key name in the metadata if the user wants a different database name
	// than the defaultSchemaName.
	keySchemaName = "schemaName"

	// The key name in the metadata for the timeout of operations, in seconds.
	keyTimeoutInSeconds = "timeoutInSeconds"

	// The key for the mandatory connection string of the metadata.
	keyConnectionString = "connectionString"

	// To connect to MySQL running in Azure over SSL you have to download a
	// SSL certificate. If this is provided the driver will connect using
	// SSL. If you have disable SSL you can leave this empty.
	// When the user provides a pem path their connection string must end with
	// &tls=custom
	// The connection string should be in the following format
	// "%s:%s@tcp(%s:3306)/%s?allowNativePasswords=true&tls=custom",'myadmin@mydemoserver', 'yourpassword', 'mydemoserver.mysql.database.azure.com', 'targetdb'.
	keyPemPath = "pemPath"

	// Used if the user does not configure a table name in the metadata.
	defaultTableName = "state"

	// Used if the user does not configure a database name in the metadata.
	defaultSchemaName = "dapr_state_store"

	// Used if the user does not provide a timeoutInSeconds value in the metadata.
	defaultTimeoutInSeconds = 20

	// Standard error message if not connection string is provided.
	errMissingConnectionString = "missing connection string"
)

// MySQL state store.
type MySQL struct {
	tableName        string
	schemaName       string
	connectionString string
	timeout          time.Duration

	// Instance of the database to issue commands to
	db *sql.DB

	// Logger used in a functions
	logger logger.Logger

	factory iMySQLFactory
}

type mySQLMetadata struct {
	TableName        string
	SchemaName       string
	ConnectionString string
	Timeout          int
	PemPath          string
}

// NewMySQLStateStore creates a new instance of MySQL state store.
func NewMySQLStateStore(logger logger.Logger) state.Store {
	factory := newMySQLFactory(logger)

	// Store the provided logger and return the object. The rest of the
	// properties will be populated in the Init function
	return newMySQLStateStore(logger, factory)
}

// Hidden implementation for testing.
func newMySQLStateStore(logger logger.Logger, factory iMySQLFactory) *MySQL {
	// Store the provided logger and return the object. The rest of the
	// properties will be populated in the Init function
	return &MySQL{
		logger:  logger,
		factory: factory,
		timeout: 5 * time.Second,
	}
}

// Init initializes the SQL server state store
// Implements the following interfaces:
// Store
// TransactionalStore
// Populate the rest of the MySQL object by reading the metadata and opening
// a connection to the server.
func (m *MySQL) Init(metadata state.Metadata) error {
	m.logger.Debug("Initializing MySql state store")

	err := m.parseMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	db, err := m.factory.Open(m.connectionString)
	if err != nil {
		m.logger.Error(err)
		return err
	}

	// will be nil if everything is good or an err that needs to be returned
	return m.finishInit(db)
}

func (m *MySQL) parseMetadata(md map[string]string) error {
	meta := mySQLMetadata{
		TableName:  defaultTableName,
		SchemaName: defaultSchemaName,
	}
	err := metadata.DecodeMetadata(md, &meta)
	if err != nil {
		return err
	}

	if meta.TableName != "" {
		// Sanitize the table name
		if !validIdentifier(meta.TableName) {
			return fmt.Errorf("table name '%s' is not valid", meta.TableName)
		}
	}
	m.tableName = meta.TableName

	if meta.SchemaName != "" {
		// Sanitize the schema name
		if !validIdentifier(meta.SchemaName) {
			return fmt.Errorf("schema name '%s' is not valid", meta.SchemaName)
		}
	}
	m.schemaName = meta.SchemaName

	if meta.ConnectionString == "" {
		m.logger.Error("Missing MySql connection string")
		return fmt.Errorf(errMissingConnectionString)
	}
	m.connectionString = meta.ConnectionString

	if meta.PemPath != "" {
		err := m.factory.RegisterTLSConfig(meta.PemPath)
		if err != nil {
			m.logger.Error(err)
			return err
		}
	}

	val, ok := md[keyTimeoutInSeconds]
	if ok && val != "" {
		n, err := strconv.Atoi(val)
		if err == nil && n > 0 {
			m.timeout = time.Duration(n) * time.Second
		}
	}
	if m.timeout <= 0 {
		m.timeout = time.Duration(defaultTimeoutInSeconds) * time.Second
	}

	return nil
}

// Features returns the features available in this state store.
func (m *MySQL) Features() []state.Feature {
	return []state.Feature{state.FeatureETag, state.FeatureTransactional}
}

// Ping the database.
func (m *MySQL) Ping() error {
	if m.db == nil {
		return sql.ErrConnDone
	}

	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	return m.db.PingContext(ctx)
}

// Separated out to make this portion of code testable.
func (m *MySQL) finishInit(db *sql.DB) error {
	m.db = db

	err := m.ensureStateSchema()
	if err != nil {
		m.logger.Error(err)
		return err
	}

	err = m.Ping()
	if err != nil {
		m.logger.Error(err)
		return err
	}

	// will be nil if everything is good or an err that needs to be returned
	return m.ensureStateTable(m.tableName)
}

func (m *MySQL) ensureStateSchema() error {
	exists, err := schemaExists(m.db, m.schemaName, m.timeout)
	if err != nil {
		return err
	}

	if !exists {
		m.logger.Infof("Creating MySql schema '%s'", m.schemaName)
		ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
		defer cancel()
		_, err = m.db.ExecContext(ctx,
			fmt.Sprintf("CREATE DATABASE %s;", m.schemaName),
		)
		if err != nil {
			return err
		}
	}

	// Build a connection string that contains the new schema name
	// All MySQL connection strings must contain a / so split on it.
	parts := strings.Split(m.connectionString, "/")

	// Even if the connection string ends with a / parts will have two values
	// with the second being an empty string.
	m.connectionString = fmt.Sprintf("%s/%s%s", parts[0], m.schemaName, parts[1])

	// Close the connection we used to confirm and or create the schema
	err = m.db.Close()

	if err != nil {
		return err
	}

	// Open a connection to the new schema
	m.db, err = m.factory.Open(m.connectionString)

	return err
}

func (m *MySQL) ensureStateTable(stateTableName string) error {
	exists, err := tableExists(m.db, stateTableName, m.timeout)
	if err != nil {
		return err
	}

	if !exists {
		m.logger.Infof("Creating MySql state table '%s'", stateTableName)

		// updateDate is updated automactically on every UPDATE commands so you
		// never need to pass it in.
		// eTag is a UUID stored as a 36 characters string. It needs to be passed
		// in on inserts and updates and is used for Optimistic Concurrency
		// Note that stateTableName is sanitized
		//nolint:gosec
		createTable := fmt.Sprintf(`CREATE TABLE %s (
			id VARCHAR(255) NOT NULL PRIMARY KEY,
			value JSON NOT NULL,
			isbinary BOOLEAN NOT NULL,
			insertDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updateDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			eTag VARCHAR(36) NOT NULL
			);`, stateTableName)

		ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
		defer cancel()
		_, err = m.db.ExecContext(ctx, createTable)

		if err != nil {
			return err
		}
	}

	return nil
}

func schemaExists(db *sql.DB, schemaName string, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Returns 1 or 0 if the table exists or not
	var exists int
	query := `SELECT EXISTS (
		SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = ?
	) AS 'exists'`
	err := db.QueryRowContext(ctx, query, schemaName).Scan(&exists)
	return exists == 1, err
}

func tableExists(db *sql.DB, tableName string, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Returns 1 or 0 if the table exists or not
	var exists int
	query := `SELECT EXISTS (
		SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME = ?
	) AS 'exists'`
	err := db.QueryRowContext(ctx, query, tableName).Scan(&exists)
	return exists == 1, err
}

// Delete removes an entity from the store
// Store Interface.
func (m *MySQL) Delete(req *state.DeleteRequest) error {
	return m.deleteValue(m.db, req)
}

func (m *MySQL) deleteValue(querier querier, req *state.DeleteRequest) error {
	m.logger.Debug("Deleting state value from MySql")

	if req.Key == "" {
		return fmt.Errorf("missing key in delete operation")
	}

	var (
		err    error
		result sql.Result
	)
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	if req.ETag == nil || *req.ETag == "" {
		result, err = querier.ExecContext(ctx, fmt.Sprintf(
			`DELETE FROM %s WHERE id = ?`,
			m.tableName), req.Key)
	} else {
		result, err = querier.ExecContext(ctx, fmt.Sprintf(
			`DELETE FROM %s WHERE id = ? and eTag = ?`,
			m.tableName), req.Key, *req.ETag)
	}

	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 && req.ETag != nil && *req.ETag != "" {
		return state.NewETagError(state.ETagMismatch, nil)
	}

	return nil
}

// BulkDelete removes multiple entries from the store
// Store Interface.
func (m *MySQL) BulkDelete(req []state.DeleteRequest) error {
	m.logger.Debug("Executing BulkDelete request")

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	if len(req) > 0 {
		for _, d := range req {
			da := d // Fix for goSec G601: Implicit memory aliasing in for loop.
			err = m.deleteValue(tx, &da)
			if err != nil {
				rollbackErr := tx.Rollback()
				if rollbackErr != nil {
					m.logger.Errorf("Error rolling back transaction: %v", rollbackErr)
				}
				return err
			}
		}
	}

	err = tx.Commit()

	return err
}

// Get returns an entity from store
// Store Interface.
func (m *MySQL) Get(req *state.GetRequest) (*state.GetResponse, error) {
	m.logger.Debug("Getting state value from MySql")

	if req.Key == "" {
		return nil, fmt.Errorf("missing key in get operation")
	}

	var (
		eTag     string
		value    []byte
		isBinary bool
	)

	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	//nolint:gosec
	query := fmt.Sprintf(
		`SELECT value, eTag, isbinary FROM %s WHERE id = ?`,
		m.tableName, // m.tableName is sanitized
	)
	err := m.db.QueryRowContext(ctx, query, req.Key).Scan(&value, &eTag, &isBinary)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return an error.
		if errors.Is(err, sql.ErrNoRows) {
			return &state.GetResponse{}, nil
		}

		return nil, err
	}

	if isBinary {
		var (
			s    string
			data []byte
		)

		err = json.Unmarshal(value, &s)
		if err != nil {
			return nil, err
		}

		data, err = base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, err
		}

		return &state.GetResponse{
			Data:     data,
			ETag:     &eTag,
			Metadata: req.Metadata,
		}, nil
	}

	return &state.GetResponse{
		Data:     value,
		ETag:     &eTag,
		Metadata: req.Metadata,
	}, nil
}

// Set adds/updates an entity on store
// Store Interface.
func (m *MySQL) Set(req *state.SetRequest) error {
	return m.setValue(m.db, req)
}

func (m *MySQL) setValue(querier querier, req *state.SetRequest) error {
	m.logger.Debug("Setting state value in MySql")

	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return errors.New("missing key in set operation")
	}

	var v any
	isBinary := false
	switch x := req.Value.(type) {
	case string:
		if x == "" {
			return errors.New("empty string is not allowed in set operation")
		}
		v = x
	case []uint8:
		isBinary = true
		v = base64.StdEncoding.EncodeToString(x)
	default:
		v = x
	}

	encB, _ := json.Marshal(v)
	enc := string(encB)

	eTagObj, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed to generate etag: %w", err)
	}
	eTag := eTagObj.String()

	var (
		result  sql.Result
		maxRows int64 = 1
	)
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || *req.ETag == "") {
		// With first-write-wins and no etag, we can insert the row only if it doesn't exist
		query := fmt.Sprintf(
			`INSERT INTO %s (value, id, eTag, isbinary) VALUES (?, ?, ?, ?);`,
			m.tableName, // m.tableName is sanitized
		)
		result, err = querier.ExecContext(ctx, query, enc, req.Key, eTag, isBinary)
	} else if req.ETag != nil && *req.ETag != "" {
		// When an eTag is provided do an update - not insert
		query := fmt.Sprintf(
			`UPDATE %s SET value = ?, eTag = ?, isbinary = ? WHERE id = ? AND eTag = ?;`,
			m.tableName, // m.tableName is sanitized
		)
		result, err = querier.ExecContext(ctx, query, enc, eTag, isBinary, req.Key, *req.ETag)
	} else {
		// If this is a duplicate MySQL returns that two rows affected
		maxRows = 2
		query := fmt.Sprintf(
			`INSERT INTO %s (value, id, eTag, isbinary) VALUES (?, ?, ?, ?) on duplicate key update value=?, eTag=?, isbinary=?;`,
			m.tableName, // m.tableName is sanitized
		)
		result, err = querier.ExecContext(ctx, query, enc, req.Key, eTag, isBinary, enc, eTag, isBinary)
	}

	if err != nil {
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		err = errors.New(`rows affected error: no rows match given key and eTag`)
		err = state.NewETagError(state.ETagMismatch, err)
		m.logger.Error(err)
		return err
	}

	if rows > maxRows {
		err = fmt.Errorf(`rows affected error: more than %d row affected; actual %d`, maxRows, rows)
		m.logger.Error(err)
		return err
	}

	return nil
}

// BulkSet adds/updates multiple entities on store
// Store Interface.
func (m *MySQL) BulkSet(req []state.SetRequest) error {
	m.logger.Debug("Executing BulkSet request")

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	if len(req) > 0 {
		for i := range req {
			err = m.setValue(tx, &req[i])
			if err != nil {
				rollbackErr := tx.Rollback()
				if rollbackErr != nil {
					m.logger.Errorf("Error rolling back transaction: %v", rollbackErr)
				}
				return err
			}
		}
	}

	return tx.Commit()
}

// Multi handles multiple transactions.
// TransactionalStore Interface.
func (m *MySQL) Multi(request *state.TransactionalStateRequest) error {
	m.logger.Debug("Executing Multi request")

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	for _, req := range request.Operations {
		switch req.Operation {
		case state.Upsert:
			setReq, err := m.getSets(req)
			if err != nil {
				rollbackErr := tx.Rollback()
				if rollbackErr != nil {
					m.logger.Errorf("Error rolling back transaction: %v", rollbackErr)
				}
				return err
			}

			err = m.setValue(tx, &setReq)
			if err != nil {
				rollbackErr := tx.Rollback()
				if rollbackErr != nil {
					m.logger.Errorf("Error rolling back transaction: %v", rollbackErr)
				}
				return err
			}

		case state.Delete:
			delReq, err := m.getDeletes(req)
			if err != nil {
				rollbackErr := tx.Rollback()
				if rollbackErr != nil {
					m.logger.Errorf("Error rolling back transaction: %v", rollbackErr)
				}
				return err
			}

			err = m.deleteValue(tx, &delReq)
			if err != nil {
				rollbackErr := tx.Rollback()
				if rollbackErr != nil {
					m.logger.Errorf("Error rolling back transaction: %v", rollbackErr)
				}
				return err
			}

		default:
			return fmt.Errorf("unsupported operation: %s", req.Operation)
		}
	}

	return tx.Commit()
}

// Returns the set requests.
func (m *MySQL) getSets(req state.TransactionalStateOperation) (state.SetRequest, error) {
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
func (m *MySQL) getDeletes(req state.TransactionalStateOperation) (state.DeleteRequest, error) {
	delReq, ok := req.Request.(state.DeleteRequest)
	if !ok {
		return delReq, fmt.Errorf("expecting delete request")
	}

	if delReq.Key == "" {
		return delReq, fmt.Errorf("missing key in delete operation")
	}

	return delReq, nil
}

// BulkGet performs a bulks get operations.
func (m *MySQL) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// by default, the store doesn't support bulk get
	// return false so daprd will fallback to call get() method one by one
	return false, nil, nil
}

// Close implements io.Closer.
func (m *MySQL) Close() error {
	if m.db == nil {
		return nil
	}

	err := m.db.Close()
	m.db = nil
	return err
}

// Validates an identifier, such as table or DB name.
// This is based on the rules for allowed unquoted identifiers (https://dev.mysql.com/doc/refman/8.0/en/identifiers.html), but more restrictive as it doesn't allow non-ASCII characters or the $ sign
func validIdentifier(v string) bool {
	if v == "" {
		return false
	}

	// Loop through the string as byte slice as we only care about ASCII characters
	b := []byte(v)
	for i := 0; i < len(b); i++ {
		if (b[i] >= '0' && b[i] <= '9') ||
			(b[i] >= 'a' && b[i] <= 'z') ||
			(b[i] >= 'A' && b[i] <= 'Z') ||
			b[i] == '_' {
			continue
		}
		return false
	}
	return true
}

// Interface for both sql.DB and sql.Tx
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (m *MySQL) GetComponentMetadata() map[string]string {
	metadataStruct := mySQLMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
