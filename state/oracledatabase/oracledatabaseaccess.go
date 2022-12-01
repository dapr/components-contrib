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
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/google/uuid"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"

	// Blank import for the underlying Oracle Database driver.
	_ "github.com/sijms/go-ora/v2"
)

const (
	connectionStringKey        = "connectionString"
	oracleWalletLocationKey    = "oracleWalletLocation"
	metadataTTLKey             = "ttlInSeconds"
	errMissingConnectionString = "missing connection string"
	tableName                  = "state"
)

// oracleDatabaseAccess implements dbaccess.
type oracleDatabaseAccess struct {
	logger           logger.Logger
	metadata         oracleDatabaseMetadata
	db               *sql.DB
	connectionString string
	tx               *sql.Tx
}

type oracleDatabaseMetadata struct {
	ConnectionString     string
	OracleWalletLocation string
	TableName            string
}

// newOracleDatabaseAccess creates a new instance of oracleDatabaseAccess.
func newOracleDatabaseAccess(logger logger.Logger) *oracleDatabaseAccess {
	logger.Debug("Instantiating new Oracle Database state store")

	return &oracleDatabaseAccess{
		logger: logger,
	}
}

func (o *oracleDatabaseAccess) Ping() error {
	return o.db.Ping()
}

func parseMetadata(meta map[string]string) (oracleDatabaseMetadata, error) {
	m := oracleDatabaseMetadata{
		TableName: "state",
	}
	err := metadata.DecodeMetadata(meta, &m)
	return m, err
}

// Init sets up OracleDatabase connection and ensures that the state table exists.
func (o *oracleDatabaseAccess) Init(metadata state.Metadata) error {
	o.logger.Debug("Initializing OracleDatabase state store")
	meta, err := parseMetadata(metadata.Properties)
	o.metadata = meta
	if err != nil {
		return err
	}
	if o.metadata.ConnectionString != "" {
		o.connectionString = meta.ConnectionString
	} else {
		o.logger.Error("Missing Oracle Database connection string")

		return fmt.Errorf(errMissingConnectionString)
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

	if pingErr := db.Ping(); pingErr != nil {
		return pingErr
	}
	err = o.ensureStateTable(tableName)
	if err != nil {
		return err
	}

	return nil
}

func parseTTL(requestMetadata map[string]string) (*int, error) {
	if val, found := requestMetadata[metadataTTLKey]; found && val != "" {
		parsedVal, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("error in parsing ttl metadata : %w", err)
		}
		parsedInt := int(parsedVal)

		return &parsedInt, nil
	}

	return nil, nil
}

// Set makes an insert or update to the database.
func (o *oracleDatabaseAccess) Set(req *state.SetRequest) error {
	o.logger.Debug("Setting state value in OracleDatabase")
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if req.Key == "" {
		return fmt.Errorf("missing key in set operation")
	}

	if v, ok := req.Value.(string); ok && v == "" {
		return fmt.Errorf("empty string is not allowed in set operation")
	}
	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || len(*req.ETag) == 0) {
		o.logger.Debugf("when FirstWrite is to be enforced, a value must be provided for the ETag")
		return fmt.Errorf("when FirstWrite is to be enforced, a value must be provided for the ETag")
	}
	var ttlSeconds int
	ttl, ttlerr := parseTTL(req.Metadata)
	if ttlerr != nil {
		return fmt.Errorf("error in parsing TTL %w", ttlerr)
	}
	if ttl != nil {
		if *ttl == -1 {
			o.logger.Debugf("TTL is set to -1; this means: never expire. ")
		} else {
			if *ttl < -1 {
				return fmt.Errorf("incorrect value for %s %d", metadataTTLKey, *ttl)
			}
			ttlSeconds = *ttl
		}
	}
	requestValue := req.Value
	byteArray, isBinary := req.Value.([]uint8)
	binaryYN := "N"
	if isBinary {
		requestValue = base64.StdEncoding.EncodeToString(byteArray)
		binaryYN = "Y"
	}

	// Convert to json string.
	bt, _ := utils.Marshal(requestValue, json.Marshal)
	value := string(bt)

	var result sql.Result
	var tx *sql.Tx
	if o.tx == nil { // not joining a preexisting transaction.
		tx, err = o.db.Begin()
		if err != nil {
			return fmt.Errorf("failed to start database transaction : %w", err)
		}
	} else { // join the transaction passed in.
		tx = o.tx
	}
	etag := uuid.New().String()
	// Only check for etag if FirstWrite specified - as per Discord message thread https://discord.com/channels/778680217417809931/901141713089863710/938520959562952735.
	if req.Options.Concurrency != state.FirstWrite {
		// Sprintf is required for table name because sql.DB does not substitute parameters for table names.
		// Other parameters use sql.DB parameter substitution.
		// As per Discord Thread https://discord.com/channels/778680217417809931/901141713089863710/938520959562952735 expiration time is reset in case of an update.
		mergeStatement := fmt.Sprintf(
			`MERGE INTO %s t using (select :key key, :value value, :binary_yn binary_yn, :etag etag , :ttl_in_seconds ttl_in_seconds from dual) new_state_to_store
			ON (t.key = new_state_to_store.key )
			WHEN MATCHED THEN UPDATE SET value = new_state_to_store.value, binary_yn = new_state_to_store.binary_yn, update_time = systimestamp, etag = new_state_to_store.etag, t.expiration_time = case when new_state_to_store.ttl_in_seconds >0 then systimestamp + numtodsinterval(new_state_to_store.ttl_in_seconds, 'SECOND') end
			WHEN NOT MATCHED THEN INSERT (t.key, t.value, t.binary_yn, t.etag, t.expiration_time) values (new_state_to_store.key, new_state_to_store.value, new_state_to_store.binary_yn, new_state_to_store.etag, case when new_state_to_store.ttl_in_seconds >0 then systimestamp + numtodsinterval(new_state_to_store.ttl_in_seconds, 'SECOND') end ) `,
			tableName)
		result, err = tx.Exec(mergeStatement, req.Key, value, binaryYN, etag, ttlSeconds)
	} else {
		// when first write policy is indicated, an existing record has to be updated - one that has the etag provided.
		// TODO: Needs to update ttl_in_seconds
		updateStatement := fmt.Sprintf(
			`UPDATE %s SET value = :value, binary_yn = :binary_yn, etag = :new_etag
			 WHERE key = :key AND etag = :etag`,
			tableName)
		result, err = tx.Exec(updateStatement, value, binaryYN, etag, req.Key, *req.ETag)
	}
	if err != nil {
		if req.ETag != nil && *req.ETag != "" {
			return state.NewETagError(state.ETagMismatch, err)
		}
		if o.tx == nil { // not in a preexisting transaction so rollback the local, failed tx.
			tx.Rollback()
		}
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if o.tx == nil { // local transaction, take responsibility.
		tx.Commit()
	}
	if rows != 1 {
		return fmt.Errorf("no item was updated")
	}
	return nil
}

// Get returns data from the database. If data does not exist for the key an empty state.GetResponse will be returned.
func (o *oracleDatabaseAccess) Get(req *state.GetRequest) (*state.GetResponse, error) {
	o.logger.Debug("Getting state value from OracleDatabase")
	if req.Key == "" {
		return nil, fmt.Errorf("missing key in get operation")
	}
	var value string
	var binaryYN string
	var etag string
	err := o.db.QueryRow(fmt.Sprintf("SELECT value, binary_yn, etag  FROM %s WHERE key = :key and (expiration_time is null or expiration_time > systimestamp)", tableName), req.Key).Scan(&value, &binaryYN, &etag)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if err == sql.ErrNoRows {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}
	if binaryYN == "Y" {
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

// Delete removes an item from the state store.
func (o *oracleDatabaseAccess) Delete(req *state.DeleteRequest) error {
	o.logger.Debug("Deleting state value from OracleDatabase")
	if req.Key == "" {
		return fmt.Errorf("missing key in delete operation")
	}
	if req.Options.Concurrency == state.FirstWrite && (req.ETag == nil || len(*req.ETag) == 0) {
		o.logger.Debugf("when FirstWrite is to be enforced, a value must be provided for the ETag")
		return fmt.Errorf("when FirstWrite is to be enforced, a value must be provided for the ETag")
	}
	var result sql.Result
	var err error
	var tx *sql.Tx
	if o.tx == nil { // not joining a preexisting transaction.
		tx, err = o.db.Begin()
		if err != nil {
			return err
		}
	} else { // join the transaction passed in.
		tx = o.tx
	}
	// QUESTION: only check for etag if FirstWrite specified - or always when etag is supplied??
	if req.Options.Concurrency != state.FirstWrite {
		result, err = tx.Exec("DELETE FROM state WHERE key = :key", req.Key)
	} else {
		result, err = tx.Exec("DELETE FROM state WHERE key = :key and etag = :etag", req.Key, *req.ETag)
	}
	if err != nil {
		if o.tx == nil { // not joining a preexisting transaction.
			tx.Rollback()
		}
		return err
	}
	if o.tx == nil { // not joining a preexisting transaction.
		tx.Commit()
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 && req.ETag != nil && *req.ETag != "" && req.Options.Concurrency == state.FirstWrite {
		return state.NewETagError(state.ETagMismatch, nil)
	}
	return nil
}

func (o *oracleDatabaseAccess) ExecuteMulti(sets []state.SetRequest, deletes []state.DeleteRequest) error {
	o.logger.Debug("Executing multiple OracleDatabase operations,  within a single transaction")
	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	o.tx = tx
	if len(deletes) > 0 {
		for _, d := range deletes {
			da := d // Fix for gosec  G601: Implicit memory aliasing in for looo.
			err = o.Delete(&da)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}
	if len(sets) > 0 {
		for _, s := range sets {
			sa := s // Fix for gosec  G601: Implicit memory aliasing in for looo.
			err = o.Set(&sa)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}
	err = tx.Commit()
	o.tx = nil
	return err
}

// Close implements io.Closer.
func (o *oracleDatabaseAccess) Close() error {
	if o.db != nil {
		return o.db.Close()
	}
	return nil
}

func (o *oracleDatabaseAccess) ensureStateTable(stateTableName string) error {
	exists, err := tableExists(o.db, stateTableName)
	if err != nil {
		return err
	}
	if !exists {
		o.logger.Info("Creating OracleDatabase state table")
		createTable := fmt.Sprintf(`CREATE TABLE %s (
									key varchar2(100) NOT NULL PRIMARY KEY,
									value clob NOT NULL,
									binary_yn varchar2(1) NOT NULL,
									etag varchar2(50)  NOT NULL,
									creation_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL ,
									expiration_time TIMESTAMP WITH TIME ZONE NULL,
									update_time TIMESTAMP WITH TIME ZONE NULL)`, stateTableName)
		_, err = o.db.Exec(createTable)
		if err != nil {
			return err
		}
	}
	return nil
}

func tableExists(db *sql.DB, tableName string) (bool, error) {
	var tblCount int32
	err := db.QueryRow("SELECT count(table_name) tbl_count FROM user_tables where table_name = upper(:tablename)", tableName).Scan(&tblCount)
	exists := tblCount > 0
	return exists, err
}
