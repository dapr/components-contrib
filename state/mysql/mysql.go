// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mysql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/agrea/ptr"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

// Optimistic Concurrency is implemented using a string column that stores
// a UUID.

const (
	// Used if the user does not configure a table name in the metadata
	defaultTableName = "state"

	// Used if the user does not configure a database name in the metadata
	defaultSchemaName = "dapr_state_store"

	// The key name in the metadata if the user wants a different table name
	// than the defaultTableName
	tableNameKey = "tableName"

	// The key name in the metadata if the user wants a different database name
	// than the defaultSchemaName
	schemaNameKey = "schemaName"

	// The key for the mandatory connection string of the metadata
	connectionStringKey = "connectionString"

	// Standard error message if not connection string is provided
	errMissingConnectionString = "missing connection string"

	// To connect to MySQL running in Azure over SSL you have to download a
	// SSL certificate. If this is provided the driver will connect using
	// SSL. If you have disable SSL you can leave this empty.
	// When the user provides a pem path their connection string must end with
	// &tls=custom
	// The connection string should be in the following format
	// "%s:%s@tcp(%s:3306)/%s?allowNativePasswords=true&tls=custom",'myadmin@mydemoserver', 'yourpassword', 'mydemoserver.mysql.database.azure.com', 'targetdb'
	pemPathKey = "pemPath"
)

// MySQL state store
type MySQL struct {
	// Name of the table to store state. If the table does not exist it will
	// be created.
	tableName string

	// Name of the table to create to store state. If the table does not exist
	// it will be created.
	schemaName string

	connectionString string

	// Instance of the database to issue commands to
	db *sql.DB

	features []state.Feature

	// Logger used in a functions
	logger logger.Logger

	factory iMySQLFactory
}

// NewMySQLStateStore creates a new instance of MySQL state store
func NewMySQLStateStore(logger logger.Logger) *MySQL {
	factory := newMySQLFactory(logger)

	// Store the provided logger and return the object. The rest of the
	// properties will be populated in the Init function
	return newMySQLStateStore(logger, factory)
}

// Hidden implementation for testing
func newMySQLStateStore(logger logger.Logger, factory iMySQLFactory) *MySQL {
	// Store the provided logger and return the object. The rest of the
	// properties will be populated in the Init function
	return &MySQL{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
		logger:   logger,
		factory:  factory,
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

	val, ok := metadata.Properties[tableNameKey]

	if ok && val != "" {
		m.tableName = val
	} else {
		// Default to the constant
		m.tableName = defaultTableName
	}

	val, ok = metadata.Properties[schemaNameKey]

	if ok && val != "" {
		m.schemaName = val
	} else {
		// Default to the constant
		m.schemaName = defaultSchemaName
	}

	m.connectionString, ok = metadata.Properties[connectionStringKey]

	if !ok || m.connectionString == "" {
		m.logger.Error("Missing MySql connection string")

		return fmt.Errorf(errMissingConnectionString)
	}

	val, ok = metadata.Properties[pemPathKey]

	if ok && val != "" {
		err := m.factory.RegisterTLSConfig(val)
		if err != nil {
			m.logger.Error(err)

			return err
		}
	}

	db, err := m.factory.Open(m.connectionString)

	// will be nil if everything is good or an err that needs to be returned
	return m.finishInit(db, err)
}

func (m *MySQL) Ping() error {
	return nil
}

// Features returns the features available in this state store
func (m *MySQL) Features() []state.Feature {
	return m.features
}

// Separated out to make this portion of code testable.
func (m *MySQL) finishInit(db *sql.DB, err error) error {
	if err != nil {
		m.logger.Error(err)

		return err
	}

	m.db = db

	schemaErr := m.ensureStateSchema()

	if schemaErr != nil {
		m.logger.Error(schemaErr)

		return schemaErr
	}

	pingErr := m.db.Ping()

	if pingErr != nil {
		m.logger.Error(pingErr)

		return pingErr
	}

	// will be nil if everything is good or an err that needs to be returned
	return m.ensureStateTable(m.tableName)
}

func (m *MySQL) ensureStateSchema() error {
	exists, err := schemaExists(m.db, m.schemaName)
	if err != nil {
		return err
	}

	if !exists {
		m.logger.Infof("Creating MySql schema '%s'", m.schemaName)

		createTable := fmt.Sprintf("CREATE DATABASE %s;", m.schemaName)

		_, err = m.db.Exec(createTable)

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
	exists, err := tableExists(m.db, stateTableName)
	if err != nil {
		return err
	}

	if !exists {
		m.logger.Infof("Creating MySql state table '%s'", stateTableName)

		// updateDate is updated automactically on every UPDATE commands so you
		// never need to pass it in.
		// eTag is a UUID stored as a 36 characters string. It needs to be passed
		// in on inserts and updates and is used for Optimistic Concurrency
		createTable := fmt.Sprintf(`CREATE TABLE %s (
			id varchar(255) NOT NULL PRIMARY KEY,
			value json NOT NULL,
			insertDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updateDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			eTag varchar(36) NOT NULL
			);`, stateTableName)

		_, err = m.db.Exec(createTable)

		if err != nil {
			return err
		}
	}

	return nil
}

func schemaExists(db *sql.DB, schemaName string) (bool, error) {
	exists := ""

	query := `SELECT EXISTS (
		SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = ?
		) AS 'exists'`

	// Returns 1 or 0 as a string if the table exists or not
	err := db.QueryRow(query, schemaName).Scan(&exists)

	return exists == "1", err
}

func tableExists(db *sql.DB, tableName string) (bool, error) {
	exists := ""

	query := `SELECT EXISTS (
		SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME = ?
		) AS 'exists'`

	// Returns 1 or 0 as a string if the table exists or not
	err := db.QueryRow(query, tableName).Scan(&exists)

	return exists == "1", err
}

// Delete removes an entity from the store
// Store Interface
func (m *MySQL) Delete(req *state.DeleteRequest) error {
	return state.DeleteWithOptions(m.deleteValue, req)
}

// deleteValue is an internal implementation of delete to enable passing the
// logic to state.DeleteWithRetries as a func.
func (m *MySQL) deleteValue(req *state.DeleteRequest) error {
	m.logger.Debug("Deleting state value from MySql")

	if req.Key == "" {
		return fmt.Errorf("missing key in delete operation")
	}

	var err error
	var result sql.Result

	if req.ETag == nil || *req.ETag == "" {
		result, err = m.db.Exec(fmt.Sprintf(
			`DELETE FROM %s WHERE id = ?`,
			m.tableName), req.Key)
	} else {
		result, err = m.db.Exec(fmt.Sprintf(
			`DELETE FROM %s WHERE id = ? and eTag = ?`,
			m.tableName), req.Key, *req.ETag)
	}

	return m.returnNDBResults(result, err, 1)
}

// BulkDelete removes multiple entries from the store
// Store Interface
func (m *MySQL) BulkDelete(req []state.DeleteRequest) error {
	return m.executeMulti(nil, req)
}

// Get returns an entity from store
// Store Interface
func (m *MySQL) Get(req *state.GetRequest) (*state.GetResponse, error) {
	m.logger.Debug("Getting state value from MySql")

	if req.Key == "" {
		return nil, fmt.Errorf("missing key in get operation")
	}

	var eTag, value string

	err := m.db.QueryRow(fmt.Sprintf(
		`SELECT value, eTag FROM %s WHERE id = ?`,
		m.tableName), req.Key).Scan(&value, &eTag)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return an error.
		if errors.Is(err, sql.ErrNoRows) {
			return &state.GetResponse{}, nil
		}

		return nil, err
	}

	response := &state.GetResponse{
		ETag:     ptr.String(eTag),
		Metadata: req.Metadata,
		Data:     []byte(value),
	}

	return response, nil
}

// Set adds/updates an entity on store
// Store Interface
func (m *MySQL) Set(req *state.SetRequest) error {
	return state.SetWithOptions(m.setValue, req)
}

// setValue is an internal implementation of set to enable passing the logic
// to state.SetWithRetries as a func.
func (m *MySQL) setValue(req *state.SetRequest) error {
	m.logger.Debug("Setting state value in MySql")

	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return fmt.Errorf("missing key in set operation")
	}

	// Convert to json string
	bt, _ := utils.Marshal(req.Value, json.Marshal)
	value := string(bt)

	var result sql.Result
	eTag := uuid.New().String()

	// Sprintf is required for table name because sql.DB does not substitute
	// parameters for table names.
	// Other parameters use sql.DB parameter substitution.
	if req.ETag == nil || *req.ETag == "" {
		// If this is a duplicate MySQL returns that two rows affected
		result, err = m.db.Exec(fmt.Sprintf(
			`INSERT INTO %s (value, id, eTag)
			 VALUES (?, ?, ?) on duplicate key update value=?, eTag=?;`,
			m.tableName), value, req.Key, eTag, value, eTag)
	} else {
		// When an eTag is provided do an update - not insert
		result, err = m.db.Exec(fmt.Sprintf(
			`UPDATE %s SET value = ?, eTag = ?
			 WHERE id = ? AND eTag = ?;`,
			m.tableName), value, eTag, req.Key, *req.ETag)
	}

	// Have to pass 2 because if the insert has a conflict MySQL returns that
	// two rows affected
	return m.returnNDBResults(result, err, 2)
}

// BulkSet adds/updates multiple entities on store
// Store Interface
func (m *MySQL) BulkSet(req []state.SetRequest) error {
	return m.executeMulti(req, nil)
}

// Multi handles multiple transactions.
// TransactionalStore Interface
func (m *MySQL) Multi(request *state.TransactionalStateRequest) error {
	var sets []state.SetRequest
	var deletes []state.DeleteRequest

	for _, req := range request.Operations {
		switch req.Operation {
		case state.Upsert:
			setReq, ok := req.Request.(state.SetRequest)

			if ok {
				sets = append(sets, setReq)
			} else {
				return fmt.Errorf("expecting set request")
			}

		case state.Delete:
			delReq, ok := req.Request.(state.DeleteRequest)

			if ok {
				deletes = append(deletes, delReq)
			} else {
				return fmt.Errorf("expecting delete request")
			}

		default:
			return fmt.Errorf("unsupported operation: %s", req.Operation)
		}
	}

	if len(sets) > 0 || len(deletes) > 0 {
		return m.executeMulti(sets, deletes)
	}

	return nil
}

// BulkGet performs a bulks get operations
func (m *MySQL) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// by default, the store doesn't support bulk get
	// return false so daprd will fallback to call get() method one by one
	return false, nil, nil
}

// Close implements io.Closer
func (m *MySQL) Close() error {
	if m.db != nil {
		return m.db.Close()
	}

	return nil
}

func (m *MySQL) executeMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error {
	m.logger.Debug("Executing multiple MySql operations")

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	if len(deletes) > 0 {
		for _, d := range deletes {
			da := d // Fix for goSec G601: Implicit memory aliasing in for loop.
			err = m.Delete(&da)
			if err != nil {
				tx.Rollback()

				return err
			}
		}
	}

	if len(sets) > 0 {
		for _, s := range sets {
			sa := s // Fix for goSec G601: Implicit memory aliasing in for loop.
			err = m.Set(&sa)
			if err != nil {
				tx.Rollback()

				return err
			}
		}
	}

	err = tx.Commit()

	return err
}

// Verifies that the sql.Result affected no more than n number of rows and no
// errors exist. If zero rows were affected something is wrong and an error
// is returned.
func (m *MySQL) returnNDBResults(result sql.Result, err error, n int64) error {
	if err != nil {
		m.logger.Debug(err)

		return err
	}

	rowsAffected, resultErr := result.RowsAffected()

	if resultErr != nil {
		m.logger.Error(resultErr)

		return resultErr
	}

	if rowsAffected == 0 {
		noRowsErr := errors.New(
			`rows affected error: no rows match given key and eTag`)
		m.logger.Error(noRowsErr)

		return noRowsErr
	}

	if rowsAffected > n {
		tooManyRowsErr := fmt.Errorf(
			`rows affected error: more than %d row affected, expected %d, actual %d`,
			n, n, rowsAffected)
		m.logger.Error(tooManyRowsErr)

		return tooManyRowsErr
	}

	return nil
}
