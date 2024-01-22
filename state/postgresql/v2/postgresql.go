/*
Copyright 2023 The Dapr Authors
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

package postgresql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	aws_credentials "github.com/aws/aws-sdk-go-v2/credentials"
	aws_auth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go/service/rds"
	awsAuth "github.com/dapr/components-contrib/common/authentication/aws"
	pginterfaces "github.com/dapr/components-contrib/common/component/postgresql/interfaces"
	pgtransactions "github.com/dapr/components-contrib/common/component/postgresql/transactions"
	sqlinternal "github.com/dapr/components-contrib/common/component/sql"
	pgmigrations "github.com/dapr/components-contrib/common/component/sql/migrations/postgres"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	createDatabaseTmpl = "CREATE DATABASE %s"
	deleteDatabaseTmpl = "DROP DATABASE IF EXISTS %s WITH (FORCE)"
)

var (
	// Define a regular expression to match the 'dbname' parameter in the connection string
	databaseNameRegex = regexp.MustCompile(`\bdbname=([^ ]+)\b`)
	userRegex         = regexp.MustCompile(`\buser=([^ ]+)\b`)
	passwordRegex     = regexp.MustCompile(`\bpassword=([^ ]+)\b`)
)

// PostgreSQL state store.
type PostgreSQL struct {
	state.BulkStore

	logger   logger.Logger
	metadata pgMetadata
	db       pginterfaces.PGXPoolConn

	gc sqlinternal.GarbageCollector

	enableAzureAD bool

	enableAWSIAM bool

	// TODO(@Sam): clean up bc I don't think I'm using this!
	awsClient *rds.RDS
}

type Options struct {
	// Disables support for authenticating with Azure AD
	// This should be set to "false" when targeting different databases than PostgreSQL (such as CockroachDB)
	NoAzureAD bool

	// Disables support for authenticating with AWS IAM
	// This should be set to "false" when targeting different databases than PostgreSQL (such as CockroachDB)
	NoAWSIAM bool
}

// NewPostgreSQLStateStore creates a new instance of PostgreSQL state store v2 with the default options.
// The v2 of the component uses a different format for storing data, always in a BYTEA column, which is more efficient than the JSONB column used in v1.
// Additionally, v2 uses random UUIDs for etags instead of the xmin column, expanding support to all Postgres-compatible databases such as CockroachDB, etc.
func NewPostgreSQLStateStore(logger logger.Logger) state.Store {
	return NewPostgreSQLStateStoreWithOptions(logger, Options{})
}

// NewPostgreSQLStateStoreWithOptions creates a new instance of PostgreSQL state store with options.
func NewPostgreSQLStateStoreWithOptions(logger logger.Logger, opts Options) state.Store {
	s := &PostgreSQL{
		logger:        logger,
		enableAzureAD: !opts.NoAzureAD,
		enableAWSIAM:  !opts.NoAWSIAM,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

func (p *PostgreSQL) parseDatabaseFromConnectionString(connectionString string) (string, error) {
	match := databaseNameRegex.FindStringSubmatch(connectionString)
	if len(match) < 2 {
		return "", fmt.Errorf("unable to find database name ('dbname' field) in the connection string")
	}

	// Return the value after "dbname="
	return match[1], nil
}

func (p *PostgreSQL) getAccessToken(ctx context.Context, pgCfg *pgx.ConnConfig) (string, error) {
	var dbEndpoint string = fmt.Sprintf("%s:%d", pgCfg.Host, pgCfg.Port)

	// https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.Go.html
	// Default to load default config through aws credentials file (~/.aws/credentials)
	awsCfg, _ := aws_config.LoadDefaultConfig(ctx)
	// TODO(@Sam): need to reallow this above!
	// if err != nil {

	// otherwise use metadata fields
	// if p.awsClient != nil {

	// Check if access key and secret access key are set
	accessKey := p.metadata.AWSAccessKey
	secretKey := p.metadata.AWSSecretKey

	// Validate if access key and secret access key are provided
	if accessKey == "" || secretKey == "" {
		return "", fmt.Errorf("failed to load default configuration for AWS using accessKey and secretKey")
	}

	// Set credentials explicitly
	var awsCfg2 aws.CredentialsProvider
	awsCfg2 = aws_credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	if awsCfg2 == nil {
		return "", fmt.Errorf("failed to get accessKey and secretKey for AWS")

	} /* else {
		return "", fmt.Errorf("failed to load default configuration for AWS: %v", err)
	}*/
	// }

	authenticationToken, err := aws_auth.BuildAuthToken(
		ctx, dbEndpoint, awsCfg.Region, pgCfg.User, awsCfg2)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS authentication token: %v", err)
	}

	return authenticationToken, nil
}

// Replace the value of 'database' with 'postgres'
func (p *PostgreSQL) getPostgresDBConnString(connString string) string {
	newConnStr := userRegex.ReplaceAllString(connString, "user=postgres")
	return databaseNameRegex.ReplaceAllString(newConnStr, "dbname=postgres")
}

func (p *PostgreSQL) getClient(dbEndpoint string) (*rds.RDS, error) {
	meta := p.metadata
	sess, err := awsAuth.GetClient(meta.AWSAccessKey, meta.AWSSecretKey, meta.AWSSessionToken, meta.AWSRegion, dbEndpoint)
	if err != nil {
		return nil, err
	}
	c := rds.New(sess)

	return c, nil
}

// Init sets up Postgres connection and performs migrations
func (p *PostgreSQL) Init(ctx context.Context, meta state.Metadata) error {
	err := p.metadata.InitWithMetadata(meta, p.enableAzureAD, p.enableAWSIAM)
	if err != nil {
		p.logger.Errorf("failed to parse metadata: %v", err)
		return err
	}

	masterConnStr := p.getPostgresDBConnString(p.metadata.ConnectionString)
	// TODO(@Sam): clean up and remove the second version of the func below!
	config, err := p.metadata.GetPgxPoolConfig2(masterConnStr)
	if err != nil {
		p.logger.Error(err)
		return err
	}

	var dbEndpoint string = fmt.Sprintf("%s:%d", config.ConnConfig.Host, config.ConnConfig.Port)
	rdsClient, err := p.getClient(dbEndpoint)
	if err != nil {
		return err
	}
	p.awsClient = rdsClient

	connCtx, connCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	p.db, err = pgxpool.NewWithConfig(connCtx, config)
	defer connCancel()
	if err != nil {
		err = fmt.Errorf("failed to connect to the database: %w", err)
		p.logger.Error(err)
		return err
	}

	// TODO(@Sam): create helpers to check & create db/user/grant roles! & consts for queries
	if p.enableAWSIAM {
		// Parse database name from connection string
		dbName, err := p.parseDatabaseFromConnectionString(p.metadata.ConnectionString)
		if err != nil {
			return fmt.Errorf("failed to parse database name from connection string to create database %v", err)
		}

		// check if database exists in connection string
		var dbExists bool
		dbExistsCtx, dbExistsCancel := context.WithTimeout(ctx, p.metadata.Timeout)
		err = p.db.QueryRow(dbExistsCtx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&dbExists)
		dbExistsCancel()
		if err != nil && err != pgx.ErrNoRows {
			return fmt.Errorf("failed to check if the PostgreSQL database %s exists: %v", dbName, err)
		}

		// Create database if needed using master password in connection string
		if !dbExists {
			createDbCtx, createDbCancel := context.WithTimeout(ctx, p.metadata.Timeout)
			_, err := p.db.Exec(createDbCtx, fmt.Sprintf(createDatabaseTmpl, dbName))
			createDbCancel()
			if err != nil {
				return fmt.Errorf("failed to create PostgreSQL user: %v", err)
			}
		}

		// Check if the user exists
		var userExists bool
		err = p.db.QueryRow(ctx, "SELECT CAST((SELECT 1 FROM pg_roles WHERE rolname = $1) AS BOOLEAN)", dbName).Scan(&userExists)
		if err != nil && err != pgx.ErrNoRows {
			return fmt.Errorf("failed to check if the PostgreSQL user %s exists: %v", dbName, err)
		}

		// Create the user if it doesn't exist
		if !userExists {
			_, err := p.db.Exec(ctx, fmt.Sprintf("CREATE USER %v", dbName))
			if err != nil {
				return fmt.Errorf("failed to create PostgreSQL user: %v", err)
			}
		}

		// Check if the role is already granted
		var roleGranted bool
		awsRole := "rds_iam"
		err = p.db.QueryRow(ctx, "SELECT 1 FROM pg_roles WHERE rolname = $1 AND 'rds_iam' = rolname", dbName).Scan(&roleGranted)
		if err != nil && err != pgx.ErrNoRows {
			return fmt.Errorf("failed to check if the role %v is already granted to the PostgreSQL user %s: %v", awsRole, dbName, err)
		}

		// Grant the role if it's not already granted
		if !roleGranted {
			_, err := p.db.Exec(ctx, fmt.Sprintf("GRANT %v TO %v", awsRole, dbName))
			if err != nil {
				return fmt.Errorf("failed to grant PostgreSQL user role: %v", err)
			}
		}

		// Set max connection lifetime to 14 minutes in postgres connection pool configuration.
		// Note: this will refresh connections before the 15 min expiration on the IAM AWS auth token,
		// while leveraging the BeforeConnect hook to recreate the token in time dynamically.
		config.MaxConnLifetime = time.Minute * 14

		// Setup connection pool config needed for AWS IAM authentication
		config.BeforeConnect = func(ctx context.Context, pgConfig *pgx.ConnConfig) error {

			// Manually reset auth token with aws and reset the config password using the new iam token
			pwd, err := p.getAccessToken(ctx, pgConfig)
			if err != nil {
				return fmt.Errorf("failed to refresh access token for iam authentication with postgresql: %v", err)
			}

			pgConfig.Password = pwd
			return nil
		}

		// TODO: add clean up hooks on user/db?
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, p.metadata.Timeout)
	err = p.db.Ping(pingCtx)
	pingCancel()
	if err != nil {
		err = fmt.Errorf("failed to ping the database: %w", err)
		p.logger.Error(err)
		return err
	}

	// Migrate schema
	err = p.performMigrations(ctx)
	if err != nil {
		p.logger.Error(err)
		return err
	}

	if p.metadata.CleanupInterval != nil {
		gc, err := sqlinternal.ScheduleGarbageCollector(sqlinternal.GCOptions{
			Logger: p.logger,
			UpdateLastCleanupQuery: func(arg any) (string, any) {
				return fmt.Sprintf(
					`INSERT INTO %[1]s (key, value)
				VALUES ('last-cleanup-state-v2-%[2]s', now()::text)
				ON CONFLICT (key)
				DO UPDATE SET value = now()::text
					WHERE (EXTRACT('epoch' FROM now() - %[1]s.value::timestamp with time zone) * 1000)::bigint > $1`,
					p.metadata.MetadataTableName,
					p.metadata.TablePrefix,
				), arg
			},
			DeleteExpiredValuesQuery: fmt.Sprintf(
				`DELETE FROM %s WHERE expires_at IS NOT NULL AND expires_at < now()`,
				p.metadata.TableName(pgTableState),
			),
			CleanupInterval: *p.metadata.CleanupInterval,
			DB:              sqlinternal.AdaptPgxConn(p.db),
		})
		if err != nil {
			return err
		}
		p.gc = gc
	}

	return nil
}

func (p *PostgreSQL) performMigrations(ctx context.Context) error {
	m := pgmigrations.Migrations{
		DB:                p.db,
		Logger:            p.logger,
		MetadataTableName: p.metadata.MetadataTableName,
		MetadataKey:       "migrations-state-v2-" + p.metadata.TablePrefix,
	}

	stateTable := p.metadata.TableName(pgTableState)

	return m.Perform(ctx, []sqlinternal.MigrationFn{
		// Migration 1: create the table for state
		func(ctx context.Context) error {
			p.logger.Infof("Creating state table: '%s'", stateTable)
			_, err := p.db.Exec(ctx,
				fmt.Sprintf(`
CREATE TABLE %[1]s (
  key text NOT NULL PRIMARY KEY,
  value bytea NOT NULL,
  etag uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  updated_at timestamp with time zone,
  expires_at timestamp with time zone
);

CREATE INDEX ON %[1]s (expires_at);
`, stateTable),
			)
			if err != nil {
				return fmt.Errorf("failed to create state table: %w", err)
			}
			return nil
		},
	})
}

// Features returns the features available in this state store.
func (p *PostgreSQL) Features() []state.Feature {
	return []state.Feature{
		state.FeatureETag,
		state.FeatureTransactional,
		state.FeatureTTL,
	}
}

func (p *PostgreSQL) GetDB() *pgxpool.Pool {
	// We can safely cast to *pgxpool.Pool because this method is never used in unit tests where we mock the DB
	return p.db.(*pgxpool.Pool)
}

// Set makes an insert or update to the database.
func (p *PostgreSQL) Set(ctx context.Context, req *state.SetRequest) error {
	if req == nil {
		return errors.New("request object is nil")
	}
	return p.doSet(ctx, p.db, *req)
}

func (p *PostgreSQL) doSet(parentCtx context.Context, db pginterfaces.DBQuerier, req state.SetRequest) error {
	if req.Key == "" {
		return errors.New("missing key in set operation")
	}

	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	// If the value is a byte slice, accept it as-is; otherwise, encode to JSON
	var value []byte
	switch x := req.Value.(type) {
	case []byte:
		value = x
	default:
		value, err = json.Marshal(x)
		if err != nil {
			return fmt.Errorf("failed to marshal to JSON: %w", err)
		}
	}

	// TTL
	var ttlSeconds int
	ttl, ttlerr := stateutils.ParseTTL(req.Metadata)
	if ttlerr != nil {
		return fmt.Errorf("error parsing TTL: %w", ttlerr)
	}
	if ttl != nil {
		ttlSeconds = *ttl
	}

	var (
		queryExpiresAt string
		params         []any
	)

	if req.HasETag() {
		// Check if the etag is valid
		var etag uuid.UUID
		etag, err = uuid.Parse(*req.ETag)
		if err != nil {
			// Return an etag mismatch error right away if the etag is invalid
			return state.NewETagError(state.ETagMismatch, err)
		}

		params = []any{req.Key, value, etag.String()}
	} else {
		params = []any{req.Key, value}
	}

	if ttlSeconds > 0 {
		queryExpiresAt = "now() + interval '" + strconv.Itoa(ttlSeconds) + " seconds'"
	} else {
		queryExpiresAt = "NULL"
	}

	// Sprintf is required for table name because the driver does not substitute parameters for table names.
	var query string
	if !req.HasETag() {
		// We do an upsert in both cases, even when concurrency is first-write, because the row may exist but be expired (and not yet garbage collected)
		// The difference is that with concurrency as first-write, we'll update the row only if it's expired
		var whereClause string
		if req.Options.Concurrency == state.FirstWrite {
			whereClause = " WHERE (t.expires_at IS NOT NULL AND t.expires_at < now())"
		}

		query = `
INSERT INTO ` + p.metadata.TableName(pgTableState) + ` AS t
  (key, value, etag, expires_at)
VALUES
  ($1, $2, gen_random_uuid(),` + queryExpiresAt + `)
ON CONFLICT (key)
DO UPDATE SET
  value = $2,
  updated_at = now(),
  etag = gen_random_uuid(),
  expires_at = ` + queryExpiresAt + whereClause
	} else {
		// When an etag is provided do an update - no insert.
		query = `
UPDATE ` + p.metadata.TableName(pgTableState) + `
SET
  value = $2,
  updated_at = now(),
  etag = gen_random_uuid(),
  expires_at = ` + queryExpiresAt + `
WHERE
  key = $1
  AND etag = $3
  AND (expires_at IS NULL OR expires_at >= now());`
	}

	result, err := db.Exec(parentCtx, query, params...)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		if req.HasETag() {
			return state.NewETagError(state.ETagMismatch, nil)
		}
		return errors.New("no item was updated")
	}

	return nil
}

// Get returns data from the database. If data does not exist for the key an empty state.GetResponse will be returned.
func (p *PostgreSQL) Get(parentCtx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req.Key == "" {
		return nil, errors.New("missing key in get operation")
	}

	var (
		value      []byte
		etag       *string
		expireTime *time.Time
	)
	query := `
SELECT
  value, etag, expires_at
FROM ` + p.metadata.TableName(pgTableState) + `
WHERE
  key = $1
  AND (expires_at IS NULL OR expires_at >= now())`

	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	defer cancel()
	row := p.db.QueryRow(ctx, query, req.Key)
	err := row.Scan(&value, &etag, &expireTime)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if errors.Is(err, pgx.ErrNoRows) {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	resp := &state.GetResponse{
		Data: value,
		ETag: etag,
	}

	if expireTime != nil {
		resp.Metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: expireTime.UTC().Format(time.RFC3339),
		}
	}

	return resp, nil
}

func (p *PostgreSQL) BulkGet(parentCtx context.Context, req []state.GetRequest, _ state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	if len(req) == 0 {
		return []state.BulkGetResponse{}, nil
	}

	// Get all keys
	keys := make([]string, len(req))
	for i, r := range req {
		keys[i] = r.Key
	}

	// Execute the query
	query := `
SELECT
  key, value, etag, expires_at
FROM ` + p.metadata.TableName(pgTableState) + `
WHERE
  key = ANY($1)
  AND (expires_at IS NULL OR expires_at >= now())`
	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	defer cancel()
	rows, err := p.db.Query(ctx, query, keys)
	if err != nil {
		return nil, err
	}

	// Scan all rows
	var n int
	res := make([]state.BulkGetResponse, len(req))
	foundKeys := make(map[string]struct{}, len(req))
	for rows.Next() {
		if n >= len(req) {
			// Sanity check to prevent panics, which should never happen
			return nil, fmt.Errorf("query returned more records than expected (expected %d)", len(req))
		}

		r := state.BulkGetResponse{}
		var expireTime *time.Time
		err = rows.Scan(&r.Key, &r.Data, &r.ETag, &expireTime)
		if err != nil {
			r.Error = err.Error()
		}
		if expireTime != nil {
			r.Metadata = map[string]string{
				state.GetRespMetaKeyTTLExpireTime: expireTime.UTC().Format(time.RFC3339),
			}
		}
		res[n] = r
		foundKeys[r.Key] = struct{}{}
		n++
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
func (p *PostgreSQL) Delete(ctx context.Context, req *state.DeleteRequest) error {
	if req == nil {
		return errors.New("request object is nil")
	}
	return p.doDelete(ctx, p.db, *req)
}

func (p *PostgreSQL) doDelete(parentCtx context.Context, db pginterfaces.DBQuerier, req state.DeleteRequest) (err error) {
	if req.Key == "" {
		return errors.New("missing key in delete operation")
	}

	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.Timeout)
	defer cancel()
	var result pgconn.CommandTag
	if req.HasETag() {
		// Check if the etag is valid
		var etag uuid.UUID
		etag, err = uuid.Parse(*req.ETag)
		if err != nil {
			// Return an etag mismatch error right away if the etag is invalid
			return state.NewETagError(state.ETagMismatch, err)
		}

		result, err = db.Exec(ctx, "DELETE FROM "+p.metadata.TableName(pgTableState)+" WHERE key = $1 AND etag = $2", req.Key, etag)
	} else {
		result, err = db.Exec(ctx, "DELETE FROM "+p.metadata.TableName(pgTableState)+" WHERE key = $1", req.Key)
	}
	if err != nil {
		return err
	}

	rows := result.RowsAffected()
	if rows != 1 && req.HasETag() {
		return state.NewETagError(state.ETagMismatch, nil)
	}

	return nil
}

func (p *PostgreSQL) Multi(parentCtx context.Context, request *state.TransactionalStateRequest) error {
	if request == nil {
		return nil
	}

	// If there's only 1 operation, skip starting a transaction
	switch len(request.Operations) {
	case 0:
		return nil
	case 1:
		return p.execMultiOperation(parentCtx, request.Operations[0], p.db)
	default:
		_, err := pgtransactions.ExecuteInTransaction[struct{}](parentCtx, p.logger, p.db, p.metadata.Timeout, func(ctx context.Context, tx pgx.Tx) (res struct{}, err error) {
			for _, op := range request.Operations {
				err = p.execMultiOperation(ctx, op, tx)
				if err != nil {
					return res, err
				}
			}

			return res, nil
		})
		return err
	}
}

func (p *PostgreSQL) execMultiOperation(ctx context.Context, op state.TransactionalStateOperation, db pginterfaces.DBQuerier) error {
	switch x := op.(type) {
	case state.SetRequest:
		return p.doSet(ctx, db, x)
	case state.DeleteRequest:
		return p.doDelete(ctx, db, x)
	default:
		return fmt.Errorf("unsupported operation: %s", op.Operation())
	}
}

func (p *PostgreSQL) CleanupExpired() error {
	if p.gc != nil {
		return p.gc.CleanupExpired()
	}
	return nil
}

// Close implements io.Close.
func (p *PostgreSQL) Close() error {
	if p.db != nil {
		p.db.Close()
		p.db = nil
	}

	if p.gc != nil {
		return p.gc.Close()
	}

	return nil
}

// GetCleanupInterval returns the cleanupInterval property.
// This is primarily used for tests.
func (p *PostgreSQL) GetCleanupInterval() *time.Duration {
	return p.metadata.CleanupInterval
}

func (p *PostgreSQL) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := pgMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}
