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

package oracledatabase

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"

	// Blank import for the underlying Oracle Database driver.
	_ "github.com/sijms/go-ora/v2"
)

const (
	connectionStringKey        = "connectionString"
	oracleWalletLocationKey    = "oracleWalletLocation"
	errMissingConnectionString = "missing connection string"
	defaultTableName           = "state"
)

// oracleDatabaseAccess implements dbaccess.
type oracleDatabaseAccess struct {
	logger           logger.Logger
	metadata         oracleDatabaseMetadata
	db               *sql.DB
	connectionString string
}

type oracleDatabaseMetadata struct {
	ConnectionString     string
	OracleWalletLocation string
	TableName            string
}

// newOracleDatabaseAccess creates a new instance of oracleDatabaseAccess.
func newOracleDatabaseAccess(logger logger.Logger) *oracleDatabaseAccess {
	return &oracleDatabaseAccess{
		logger: logger,
	}
}

func (o *oracleDatabaseAccess) Ping(ctx context.Context) error {
	return o.db.PingContext(ctx)
}

func parseMetadata(meta map[string]string) (oracleDatabaseMetadata, error) {
	m := oracleDatabaseMetadata{
		TableName: defaultTableName,
	}
	err := metadata.DecodeMetadata(meta, &m)
	return m, err
}

// Init sets up OracleDatabase connection and ensures that the state table exists.
func (o *oracleDatabaseAccess) Init(ctx context.Context, metadata state.Metadata) error {
	meta, err := parseMetadata(metadata.Properties)
	o.metadata = meta
	if err != nil {
		return err
	}
	if o.metadata.ConnectionString != "" {
		o.connectionString = meta.ConnectionString
	} else {
		o.logger.Error("Missing Oracle Database connection string")
		return errors.New(errMissingConnectionString)
	}
	if o.metadata.OracleWalletLocation != "" {
		o.connectionString += "?TRACE FILE=trace.log&SSL=enable&SSL Verify=false&WALLET=" + url.QueryEscape(o.metadata.OracleWalletLocation)
	}
	db, err := sql.Open("oracle", o.connectionString)
	if err != nil {
		o.logger.Error(err)

		return err
	}

	o.db = db

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	err = o.ensureStateTable(o.metadata.TableName)
	if err != nil {
		return err
	}

	return nil
}

// Set makes an insert or update to the database.
func (o *oracleDatabaseAccess) Set(ctx context.Context, req *state.SetRequest) error {
	return o.doSet(ctx, o.db, req)
}

func (o *oracleDatabaseAccess) doSet(ctx context.Context, db querier, req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return errors.New("missing key in set operation")
	}

	requestValue := req.Value
	byteArray, isBinary := req.Value.([]uint8)
	binaryYN := "N"
	if isBinary {
		requestValue = base64.StdEncoding.EncodeToString(byteArray)
		binaryYN = "Y"
	}

	// Convert to json string
	bt, _ := stateutils.Marshal(requestValue, json.Marshal)
	value := string(bt)

	// TTL
	var ttlSeconds int
	ttl, err := stateutils.ParseTTL(req.Metadata)
	if err != nil {
		return fmt.Errorf("error parsing TTL: %w", err)
	}
	if ttl != nil {
		ttlSeconds = *ttl
	}
	ttlStatement := "NULL"
	if ttlSeconds > 0 {
		// We're passing ttlStatements via string concatenation - no risk of SQL injection because we control the value and make sure it's a number
		ttlStatement = "systimestamp + numtodsinterval(" + strconv.Itoa(ttlSeconds) + ", 'SECOND')"
	}

	// Generate new etag
	etagObj, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed to generate UUID: %w", err)
	}
	etag := etagObj.String()

	var result sql.Result
	if !req.HasETag() {
		// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
		// Other parameters use sql.DB parameter substitution.
		var stmt string
		if req.Options.Concurrency == state.FirstWrite {
			stmt = `INSERT INTO ` + o.metadata.TableName + `
				(key, value, binary_yn, etag, expiration_time)
			VALUES 
				(:key, :value, :binary_yn, :etag, ` + ttlStatement + `) `
		} else {
			// As per Discord Thread https://discord.com/channels/778680217417809931/901141713089863710/938520959562952735 expiration time is reset in case of an update.
			stmt = `MERGE INTO ` + o.metadata.TableName + ` t
				USING (SELECT :key key, :value value, :binary_yn binary_yn, :etag etag FROM dual) new_state_to_store
				ON (t.key = new_state_to_store.key)
				WHEN MATCHED THEN UPDATE SET value = new_state_to_store.value, binary_yn = new_state_to_store.binary_yn, update_time = systimestamp, etag = new_state_to_store.etag, t.expiration_time = ` + ttlStatement + `
				WHEN NOT MATCHED THEN INSERT (t.key, t.value, t.binary_yn, t.etag, t.expiration_time) VALUES (new_state_to_store.key, new_state_to_store.value, new_state_to_store.binary_yn, new_state_to_store.etag, ` + ttlStatement + ` ) `
		}
		result, err = db.ExecContext(ctx, stmt, req.Key, value, binaryYN, etag)
	} else {
		// When first write policy is indicated, an existing record has to be updated - one that has the etag provided.
		updateStatement := `UPDATE ` + o.metadata.TableName + ` SET
			  value = :value,
			  binary_yn = :binary_yn,
			  etag = :new_etag,
			  update_time = systimestamp,
			  expiration_time = ` + ttlStatement + `
			 WHERE key = :key
			   AND etag = :etag
			   AND (expiration_time IS NULL OR expiration_time >= systimestamp)`
		result, err = db.ExecContext(ctx, updateStatement, value, binaryYN, etag, req.Key, *req.ETag)
	}
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		if req.HasETag() {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return errors.New("no item was updated")
	}
	return nil
}

// Get returns data from the database. If data does not exist for the key an empty state.GetResponse will be returned.
func (o *oracleDatabaseAccess) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req.Key == "" {
		return nil, errors.New("missing key in get operation")
	}
	var (
		value      string
		binaryYN   string
		etag       string
		expireTime sql.NullTime
	)
	err := o.db.QueryRowContext(ctx, "SELECT value, binary_yn, etag, expiration_time FROM "+o.metadata.TableName+" WHERE key = :key AND (expiration_time IS NULL OR expiration_time > systimestamp)", req.Key).Scan(&value, &binaryYN, &etag, &expireTime)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if err == sql.ErrNoRows {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	var metadata map[string]string
	if expireTime.Valid {
		metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: expireTime.Time.UTC().Format(time.RFC3339),
		}
	}
	if binaryYN == "Y" {
		var (
			s    string
			data []byte
		)
		if err = json.Unmarshal([]byte(value), &s); err != nil {
			return nil, err
		}
		if data, err = base64.StdEncoding.DecodeString(s); err != nil {
			return nil, err
		}
		return &state.GetResponse{
			Data:     data,
			ETag:     &etag,
			Metadata: metadata,
		}, nil
	}
	return &state.GetResponse{
		Data:     []byte(value),
		ETag:     &etag,
		Metadata: metadata,
	}, nil
}

// Delete removes an item from the state store.
func (o *oracleDatabaseAccess) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return o.doDelete(ctx, o.db, req)
}

func (o *oracleDatabaseAccess) doDelete(ctx context.Context, db querier, req *state.DeleteRequest) (err error) {
	if req.Key == "" {
		return errors.New("missing key in delete operation")
	}

	var result sql.Result
	if !req.HasETag() {
		result, err = db.ExecContext(ctx, "DELETE FROM "+o.metadata.TableName+" WHERE key = :key", req.Key)
	} else {
		result, err = db.ExecContext(ctx, "DELETE FROM "+o.metadata.TableName+" WHERE key = :key AND etag = :etag", req.Key, *req.ETag)
	}
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 && req.ETag != nil && *req.ETag != "" {
		return state.NewETagError(state.ETagMismatch, nil)
	}
	return nil
}

func (o *oracleDatabaseAccess) ExecuteMulti(parentCtx context.Context, reqs []state.TransactionalStateOperation) error {
	tx, err := o.db.BeginTx(parentCtx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, op := range reqs {
		switch req := op.(type) {
		case state.SetRequest:
			err = o.doSet(parentCtx, tx, &req)
			if err != nil {
				return err
			}
		case state.DeleteRequest:
			err = o.doDelete(parentCtx, tx, &req)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported operation: %s", req.Operation())
		}
	}
	return tx.Commit()
}

// Close implements io.Closer.
func (o *oracleDatabaseAccess) Close() error {
	if o.db != nil {
		return o.db.Close()
	}
	return nil
}

func (o *oracleDatabaseAccess) ensureStateTable(stateTableName string) error {
	// TODO: This is not atomic and can lead to race conditions if multiple sidecars are starting up at the same time
	exists, err := tableExists(o.db, stateTableName)
	if err != nil {
		return err
	}
	if !exists {
		o.logger.Info("Creating state table")
		createTable := `CREATE TABLE ` + stateTableName + ` (
						key varchar2(100) NOT NULL PRIMARY KEY,
						value clob NOT NULL,
						binary_yn varchar2(1) NOT NULL,
						etag varchar2(50)  NOT NULL,
						creation_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL ,
						expiration_time TIMESTAMP WITH TIME ZONE NULL,
						update_time TIMESTAMP WITH TIME ZONE NULL)`
		_, err = o.db.Exec(createTable)
		if err != nil {
			return err
		}
	}
	return nil
}

func tableExists(db *sql.DB, tableName string) (bool, error) {
	var tblCount int32
	err := db.QueryRow("SELECT count(table_name) tbl_count FROM user_tables WHERE table_name = upper(:tablename)", tableName).Scan(&tblCount)
	exists := tblCount > 0
	return exists, err
}
