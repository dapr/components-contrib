package sqlite

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"

	// Blank import for the underlying SQLite Driver.
	_ "github.com/mattn/go-sqlite3"
)

// sqliteDBAccess implements dbaccess.
type sqliteDBAccess struct {
	logger           logger.Logger
	metadata         state.Metadata
	connectionString string
	tableName        string
	db               *sql.DB
	cleanupInterval  *time.Duration

	// Lock only on public write API. Any public API's implementation should not call other public write APIs.
	writeMu *sync.Mutex
}

// newSqliteDBAccess creates a new instance of sqliteDbAccess.
func newSqliteDBAccess(logger logger.Logger) *sqliteDBAccess {
	logger.Debug("Instantiating new SQLite state store")

	return &sqliteDBAccess{
		logger:  logger,
		writeMu: &sync.Mutex{},
	}
}

// Init sets up SQLite Database connection and ensures that the state table
// exists.
func (a *sqliteDBAccess) Init(metadata state.Metadata) error {
	a.logger.Debug("Initializing SQLite state store")
	a.metadata = metadata

	tableName, ok := metadata.Properties[tableNameKey]
	if !ok || tableName == "" {
		tableName = defaultTableName
	}
	a.tableName = tableName

	cleanupInterval, err := a.parseCleanupInterval(metadata)
	if err != nil {
		return err
	}
	a.cleanupInterval = cleanupInterval

	if val, ok := metadata.Properties[connectionStringKey]; ok && val != "" {
		a.connectionString = val
	} else {
		a.logger.Error("Missing SQLite connection string")

		return fmt.Errorf(errMissingConnectionString)
	}

	db, err := sql.Open("sqlite3", a.connectionString)
	if err != nil {
		a.logger.Error(err)
		return err
	}

	a.db = db

	if pingErr := db.Ping(); pingErr != nil {
		return pingErr
	}

	err = a.ensureStateTable(tableName)
	if err != nil {
		return err
	}

	a.scheduleCleanupExpiredData()

	return nil
}

func (a *sqliteDBAccess) Ping() error {
	return a.db.Ping()
}

func (a *sqliteDBAccess) Get(req *state.GetRequest) (*state.GetResponse, error) {
	a.logger.Debug("Get state value from SQLite")
	if req.Key == "" {
		return nil, fmt.Errorf("missing key in get operation")
	}
	var value string
	var isBinary bool
	var etag string

	// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
	stmt := fmt.Sprintf(getValueTpl, a.tableName)
	err := a.db.QueryRow(stmt, req.Key).Scan(&value, &isBinary, &etag)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &state.GetResponse{
				Metadata: req.Metadata,
			}, nil
		}
		return nil, err
	}
	if isBinary {
		var s string
		var data []byte
		if err = json.Unmarshal([]byte(value), &s); err != nil {
			return nil, err
		}
		if data, err = base64.StdEncoding.DecodeString(s); err != nil {
			return nil, err
		}
		return &state.GetResponse{
			Data:     data,
			ETag:     &etag,
			Metadata: req.Metadata,
		}, nil
	}
	return &state.GetResponse{
		Data:     []byte(value),
		ETag:     &etag,
		Metadata: req.Metadata,
	}, nil
}

func (a *sqliteDBAccess) Set(req *state.SetRequest) error {
	a.logger.Debug("Set state value in SQLite")
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	tx, err := a.db.Begin()
	if err != nil {
		return err
	}

	err = state.SetWithOptions(func(req *state.SetRequest) error { return a.setValue(tx, req) }, req)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (a *sqliteDBAccess) Delete(req *state.DeleteRequest) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	tx, err := a.db.Begin()
	if err != nil {
		return err
	}

	err = state.DeleteWithOptions(func(req *state.DeleteRequest) error { return a.deleteValue(tx, req) }, req)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (a *sqliteDBAccess) ExecuteMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error {
	a.logger.Debug("Executing multiple SQLite operations, within a single transaction")
	a.writeMu.Lock()
	defer a.writeMu.Unlock()

	tx, err := a.db.Begin()
	if err != nil {
		return err
	}

	if len(deletes) > 0 {
		for _, d := range deletes {
			da := d // Fix for gosec  G601: Implicit memory aliasing in for loop.
			err = a.deleteValue(tx, &da)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}
	if len(sets) > 0 {
		for _, s := range sets {
			sa := s // Fix for gosec  G601: Implicit memory aliasing in for loop.
			err = a.setValue(tx, &sa)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}
	return tx.Commit()
}

// Close implements io.Close.
func (a *sqliteDBAccess) Close() error {
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

// Create table if not exists.
func (a *sqliteDBAccess) ensureStateTable(stateTableName string) error {
	exists, err := tableExists(a.db, stateTableName)
	if err != nil {
		return err
	}

	if !exists {
		a.logger.Infof("Creating SQLite state table '%s'", stateTableName)
		tx, err := a.db.Begin()
		if err != nil {
			return err
		}

		stmt := fmt.Sprintf(createTableTpl, stateTableName)
		_, err = tx.Exec(stmt)
		if err != nil {
			tx.Rollback()
			return err
		}

		stmt = fmt.Sprintf(createTableExpirationTimeIdx, stateTableName, stateTableName)
		_, err = tx.Exec(stmt)
		if err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	}

	return nil
}

// Check if table exists.
func tableExists(db *sql.DB, tableName string) (bool, error) {
	exists := ""
	// Returns 1 or 0 as a string if the table exists or not.
	err := db.QueryRow(tableExistsStmt, tableName).Scan(&exists)
	return exists == "1", err
}

func (a *sqliteDBAccess) setValue(tx *sql.Tx, req *state.SetRequest) error {
	r, err := prepareSetRequest(a, tx, req)
	if err != nil {
		return err
	}

	hasUpdate, err := r.setValue()
	if err != nil {
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return err
	}

	if !hasUpdate {
		return fmt.Errorf("no item was updated")
	}
	return nil
}

func (a *sqliteDBAccess) deleteValue(tx *sql.Tx, req *state.DeleteRequest) error {
	a.logger.Debug("Deleting state value from SQLite")
	r, err := prepareDeleteRequest(a, tx, req)
	if err != nil {
		return err
	}

	hasUpdate, err := r.deleteValue()
	if err != nil {
		return err
	}

	if !hasUpdate && req.ETag != nil && *req.ETag != "" && req.Options.Concurrency == state.FirstWrite {
		return state.NewETagError(state.ETagMismatch, nil)
	}
	return nil
}

func (a *sqliteDBAccess) scheduleCleanupExpiredData() {
	if a.cleanupInterval == nil {
		return
	}

	d := *a.cleanupInterval
	a.logger.Infof("Schedule timeouted data clean up every %d seconds", d)
	ticker := time.NewTicker(d)
	go func() {
		for range ticker.C {
			a.cleanupTimeout()
		}
	}()
}

func (a *sqliteDBAccess) cleanupTimeout() {
	if !a.writeMu.TryLock() {
		return
	}
	defer a.writeMu.Unlock()
	tx, err := a.db.Begin()
	if err != nil {
		a.logger.Errorf("Error cleanup SQLite state store timeout data", err)
		return
	}

	stmt := fmt.Sprintf(cleanupTimeoutStmtTpl, a.tableName)
	res, err := tx.Exec(stmt)
	if err != nil {
		a.logger.Errorf("Error cleanup SQLite state store timeout data", err)
		return
	}

	cleaned, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		a.logger.Errorf("Error cleanup SQLite state store timeout data", err)
		return
	}

	err = tx.Commit()
	if err != nil {
		a.logger.Errorf("Error cleanup SQLite state store timeout data", err)
		return
	}

	a.logger.Debugf("Cleaned %d timeout data from SQLite state store", cleaned)
}

// Returns nil duration means never cleanup timeouted data.
func (a *sqliteDBAccess) parseCleanupInterval(metadata state.Metadata) (*time.Duration, error) {
	s, ok := metadata.Properties[cleanupIntervalKey]
	if ok && s != "" {
		cleanupIntervalInSec, err := strconv.ParseInt(s, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("illegal cleanupIntervalInSec value: %s", s)
		}

		// Non-postitive value from meta means disable auto cleanup.
		if cleanupIntervalInSec > 0 {
			d := time.Duration(cleanupIntervalInSec) * time.Second
			return &d, nil
		}
	} else {
		d := defaultCleanupInternalInSec * time.Second
		return &d, nil
	}

	return nil, nil
}
