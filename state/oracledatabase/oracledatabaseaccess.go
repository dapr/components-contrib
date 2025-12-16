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
	"strings"
	"time"

	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/metadata"

	"github.com/google/uuid"
	goora "github.com/sijms/go-ora/v2"
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
	ConnectionString     string `json:"connectionString"`
	OracleWalletLocation string `json:"oracleWalletLocation"`
	TableName            string `json:"tableName"`
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
	if err != nil {
		return err
	}

	o.metadata = meta

	if o.metadata.ConnectionString == "" {
		o.logger.Error("Missing Oracle Database connection string")
		return errors.New(errMissingConnectionString)
	}

	o.connectionString, err = parseConnectionString(meta)
	if err != nil {
		o.logger.Error(err)
		return err
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

	return o.ensureStateTable(o.metadata.TableName)
}

func parseConnectionString(meta oracleDatabaseMetadata) (string, error) {
	username := ""
	password := ""
	host := ""
	port := 0
	serviceName := ""
	query := url.Values{}
	options := make(map[string]string)

	connectionStringURL, err := url.Parse(meta.ConnectionString)
	if err != nil {
		return "", err
	}

	isURL := connectionStringURL.Scheme != "" && connectionStringURL.Host != ""
	if isURL {
		username = connectionStringURL.User.Username()
		password, _ = connectionStringURL.User.Password()
		query = connectionStringURL.Query()
		serviceName = strings.TrimPrefix(connectionStringURL.Path, "/")
		if strings.Contains(connectionStringURL.Host, ":") {
			host = strings.Split(connectionStringURL.Host, ":")[0]
		} else {
			host = connectionStringURL.Host
		}
	} else {
		host = connectionStringURL.Path
	}

	if connectionStringURL.Port() != "" {
		port, err = strconv.Atoi(connectionStringURL.Port())
		if err != nil {
			return "", err
		}
	}

	for k, v := range query {
		options[k] = v[0]
	}

	if meta.OracleWalletLocation != "" {
		options["WALLET"] = meta.OracleWalletLocation
		options["TRACE FILE"] = "trace.log"
		options["SSL"] = "enable"
		options["SSL Verify"] = "false"
	}

	if strings.Contains(host, "(DESCRIPTION") {
		// the connection string is a URL that contains the descriptor and authentication info
		return goora.BuildJDBC(username, password, host, options), nil
	} else {
		return goora.BuildUrl(host, port, serviceName, username, password, options), nil
	}
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

func (o *oracleDatabaseAccess) BulkGet(ctx context.Context, req []state.GetRequest) ([]state.BulkGetResponse, error) {
	if len(req) == 0 {
		return []state.BulkGetResponse{}, nil
	}

	// Oracle supports the IN operator for bulk operations
	// Build the IN clause with bind variables
	// Oracle uses :1, :2, etc. for bind variables in the IN clause
	params := make([]any, len(req))
	bindVars := make([]string, len(req))
	for i, r := range req {
		if r.Key == "" {
			return nil, errors.New("missing key in bulk get operation")
		}
		params[i] = r.Key
		bindVars[i] = ":" + strconv.Itoa(i+1)
	}

	inClause := strings.Join(bindVars, ",")
	// Concatenation is required for table name because sql.DB does not substitute parameters for table names.
	//nolint:gosec
	query := "SELECT key, value, binary_yn, etag, expiration_time FROM " + o.metadata.TableName + " WHERE key IN (" + inClause + ") AND (expiration_time IS NULL OR expiration_time > systimestamp)"

	rows, err := o.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var n int
	res := make([]state.BulkGetResponse, len(req))
	foundKeys := make(map[string]struct{}, len(req))

	for rows.Next() {
		if n >= len(req) {
			// Sanity check to prevent panics, which should never happen
			return nil, fmt.Errorf("query returned more records than expected (expected %d)", len(req))
		}

		var (
			key        string
			value      string
			binaryYN   string
			etag       string
			expireTime sql.NullTime
		)

		err = rows.Scan(&key, &value, &binaryYN, &etag, &expireTime)
		if err != nil {
			res[n] = state.BulkGetResponse{
				Key:   key,
				Error: err.Error(),
			}
		} else {
			response := state.BulkGetResponse{
				Key:  key,
				ETag: &etag,
			}

			if expireTime.Valid {
				response.Metadata = map[string]string{
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
				response.Data = data
			} else {
				response.Data = []byte(value)
			}

			res[n] = response
		}

		foundKeys[key] = struct{}{}
		n++
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	// Populate missing keys with empty values
	// This is to ensure consistency with the other state stores that implement BulkGet as a loop over Get, and with the Get method
	if len(foundKeys) < len(req) {
		var ok bool
		for _, r := range req {
			_, ok = foundKeys[r.Key]
			if !ok {
				if n >= len(req) {
					// Sanity check to prevent panics, which should never happen
					return nil, fmt.Errorf("query returned more records than expected (expected %d)", len(req))
				}
				res[n] = state.BulkGetResponse{
					Key: r.Key,
				}
				n++
			}
		}
	}

	return res[:n], nil
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
	//nolint:gosec
	query := fmt.Sprintf("SELECT 1 FROM %s WHERE ROWNUM = 1", tableName)

	var dummy int
	err := db.QueryRow(query).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil // Table exists but is empty
		}
		return false, nil // Likely a table does not exist error
	}
	return true, nil
}

func (o *oracleDatabaseAccess) KeysLike(ctx context.Context, req state.KeysLikeRequest) (*state.KeysLikeResponse, error) {
	if o.db == nil {
		return nil, errors.New("oracle db not initialized")
	}

	table := o.metadata.TableName

	baseWhere := " WHERE key LIKE :pat ESCAPE '\\' AND (expiration_time IS NULL OR expiration_time > SYSTIMESTAMP) "

	args := []any{req.Pattern}

	seek := ""
	if req.ContinuationToken != nil && *req.ContinuationToken != "" {
		seek = " AND key > :token "
		args = append(args, *req.ContinuationToken)
	}

	orderBy := " ORDER BY key ASC "

	var query string
	var pageSize uint32

	if req.PageSize != nil && *req.PageSize > 0 {
		pageSize = *req.PageSize
		take := int64(pageSize + 1)

		query = fmt.Sprintf(`
SELECT key FROM (
  SELECT key
  FROM %s
  %s%s%s
)
WHERE ROWNUM <= :take
`, table, baseWhere, seek, orderBy)

		args = append(args, take)
	} else {
		query = fmt.Sprintf(`
SELECT key
FROM %s
%s%s%s
`, table, baseWhere, seek, orderBy)
	}

	rows, err := o.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make([]string, 0, 256)
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	resp := &state.KeysLikeResponse{
		Keys: make([]string, 0, len(keys)),
	}

	//nolint:gosec
	if pageSize > 0 && uint32(len(keys)) > pageSize {
		next := keys[pageSize]
		resp.ContinuationToken = &next
		keys = keys[:pageSize]
	}

	resp.Keys = append(resp.Keys, keys...)
	return resp, nil
}
