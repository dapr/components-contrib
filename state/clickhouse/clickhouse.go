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

package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
)

type StateStore struct {
	db     *sql.DB
	logger logger.Logger
	config clickhouseMetadata
}

type clickhouseMetadata struct {
	ClickHouseURL string
	Database      string
	Table         string
	Username      string
	Password      string
}

func NewClickHouseStateStore(logger logger.Logger) state.Store {
	return &StateStore{
		logger: logger,
	}
}

func (c *StateStore) Init(ctx context.Context, metadata state.Metadata) error {
	config, err := parseAndValidateMetadata(metadata)
	if err != nil {
		return err
	}
	c.config = config

	// Construct DSN with authentication if provided
	dsn := c.config.ClickHouseURL
	// If username and password are provided and not already in the URL, add them to the DSN
	if c.config.Username != "" && !strings.Contains(dsn, "username=") {
		if !strings.Contains(dsn, "?") {
			dsn += "?"
		} else {
			dsn += "&"
		}
		dsn += "username=" + c.config.Username
	}

	if c.config.Password != "" && !strings.Contains(dsn, "password=") {
		if !strings.Contains(dsn, "?") {
			dsn += "?"
		} else if !strings.HasSuffix(dsn, "&") {
			dsn += "&"
		}
		dsn += "password=" + c.config.Password
	}

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("error opening connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}

	// Create database if not exists
	createDBQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.config.Database)
	if _, err := db.ExecContext(ctx, createDBQuery); err != nil {
		return fmt.Errorf("error creating database: %v", err)
	}

	// Create table if not exists with ReplacingMergeTree
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			key String,
			value String,
			etag String,
			expire DateTime64(3) NULL,
			PRIMARY KEY(key)
		) ENGINE = ReplacingMergeTree()
		ORDER BY key
	`, c.config.Database, c.config.Table)

	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	c.db = db
	return nil
}

func (c *StateStore) Features() []state.Feature {
	return []state.Feature{
		state.FeatureETag,
		state.FeatureTTL,
	}
}

func (c *StateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req.Key == "" {
		return nil, errors.New("key is empty")
	}

	query := fmt.Sprintf(`
		SELECT value, etag, expire 
		FROM %s.%s FINAL  -- Add FINAL to get the latest version
		WHERE key = ? AND (expire IS NULL OR expire > now64())
	`, c.config.Database, c.config.Table)
	
	var value, etag string
	var expire *time.Time
	err := c.db.QueryRowContext(ctx, query, req.Key).Scan(&value, &etag, &expire)
	if err == sql.ErrNoRows {
		return &state.GetResponse{}, nil
	}
	if err != nil {
		return nil, err
	}

	var metadata map[string]string
	if expire != nil {
		metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: expire.UTC().Format(time.RFC3339),
		}
	}

	return &state.GetResponse{
		Data:     []byte(value),
		ETag:     &etag,
		Metadata: metadata,
	}, nil
}

func (c *StateStore) Set(ctx context.Context, req *state.SetRequest) error {
	if req.Key == "" {
		return errors.New("key is empty")
	}

	ttlInSeconds := 0
	if req.Metadata != nil {
		var err error
		ttlInSeconds, err = parseTTL(req.Metadata)
		if err != nil {
			return err
		}
	}

	value, err := c.marshal(req.Value)
	if err != nil {
		return err
	}

	var expireTime *time.Time
	if ttlInSeconds > 0 {
		t := time.Now().Add(time.Duration(ttlInSeconds) * time.Second)
		expireTime = &t
	}

	// Handle ETag for optimistic concurrency
	if req.ETag != nil && *req.ETag != "" {
		// First, get the current etag
		currentETag, err := c.getETag(ctx, req.Key)
		if err != nil {
			return err
		}

		// If an etag exists and it doesn't match the provided etag, return error
		if currentETag != "" && currentETag != *req.ETag {
			return state.NewETagError(state.ETagMismatch, nil)
		}
	} else if req.Options.Concurrency == state.FirstWrite {
		// Check if the key already exists for first-write
		exists, err := c.keyExists(ctx, req.Key)
		if err != nil {
			return err
		}
		if exists {
			return state.NewETagError(state.ETagMismatch, nil)
		}
	}

	// Generate a new etag for this write
	etag := uuid.New().String()

	// ClickHouse uses ALTER TABLE ... UPDATE instead of ON DUPLICATE KEY
	// First try to insert
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s (key, value, etag, expire)
		VALUES (?, ?, ?, ?)
	`, c.config.Database, c.config.Table)

	_, err = c.db.ExecContext(ctx, insertQuery, req.Key, value, etag, expireTime)
	if err != nil {
		// If the key exists, update it
		updateQuery := fmt.Sprintf(`
			ALTER TABLE %s.%s 
			UPDATE value = ?, etag = ?, expire = ?
			WHERE key = ?
		`, c.config.Database, c.config.Table)

		_, err = c.db.ExecContext(ctx, updateQuery, value, etag, expireTime, req.Key)
		if err != nil {
			return fmt.Errorf("error updating value: %v", err)
		}
	}

	return nil
}

func (c *StateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	if req.Key == "" {
		return errors.New("key is empty")
	}

	// Handle ETag for optimistic concurrency
	if req.ETag != nil && *req.ETag != "" {
		// First, get the current etag
		currentETag, err := c.getETag(ctx, req.Key)
		if err != nil {
			return err
		}

		// If an etag exists and it doesn't match the provided etag, return error
		if currentETag != "" && currentETag != *req.ETag {
			return state.NewETagError(state.ETagMismatch, nil)
		}
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE key = ?", c.config.Database, c.config.Table)
	_, err := c.db.ExecContext(ctx, query, req.Key)
	return err
}

func (c *StateStore) marshal(v any) (string, error) {
	var value string
	switch v := v.(type) {
	case []byte:
		value = string(v)
	case string:
		value = v
	default:
		bt, err := utils.Marshal(v, json.Marshal)
		if err != nil {
			return "", err
		}
		value = string(bt)
	}
	return value, nil
}

func parseTTL(metadata map[string]string) (int, error) {
	if metadata == nil {
		return 0, nil
	}
	ttl, ok := metadata["ttlInSeconds"]
	if !ok || ttl == "" {
		return 0, nil
	}
	
	ttlMetadata := map[string]string{
		"ttlInSeconds": ttl,
	}
	
	ttlPtr, err := utils.ParseTTL(ttlMetadata)
	if err != nil {
		return 0, fmt.Errorf("error parsing TTL: %v", err)
	}
	
	if ttlPtr == nil {
		return 0, nil
	}
	
	return *ttlPtr, nil
}

func parseAndValidateMetadata(metadata state.Metadata) (clickhouseMetadata, error) {
	config := clickhouseMetadata{}

	if val, ok := metadata.Properties["clickhouseURL"]; ok && val != "" {
		config.ClickHouseURL = val
	} else {
		return config, errors.New("ClickHouse URL is missing")
	}

	if val, ok := metadata.Properties["databaseName"]; ok && val != "" {
		config.Database = val
	} else {
		return config, errors.New("ClickHouse database name is missing")
	}

	if val, ok := metadata.Properties["tableName"]; ok && val != "" {
		config.Table = val
	} else {
		return config, errors.New("ClickHouse table name is missing")
	}

	// Get username and password if provided
	if val, ok := metadata.Properties["username"]; ok {
		config.Username = val
	}

	if val, ok := metadata.Properties["password"]; ok {
		config.Password = val
	}

	return config, nil
}

func (c *StateStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := clickhouseMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func (c *StateStore) BulkGet(ctx context.Context, reqs []state.GetRequest, opts state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	responses := make([]state.BulkGetResponse, len(reqs))
	for i, req := range reqs {
		response, err := c.Get(ctx, &req)
		if err != nil {
			return nil, err
		}
		responses[i] = state.BulkGetResponse{
			Key:  req.Key,
			Data: response.Data,
			ETag: response.ETag,
			Metadata: response.Metadata,
			Error: "",
		}
	}
	return responses, nil
}

func (c *StateStore) BulkSet(ctx context.Context, reqs []state.SetRequest, opts state.BulkStoreOpts) error {
	for _, req := range reqs {
		err := c.Set(ctx, &req)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *StateStore) BulkDelete(ctx context.Context, reqs []state.DeleteRequest, opts state.BulkStoreOpts) error {
	for _, req := range reqs {
		err := c.Delete(ctx, &req)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *StateStore) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// getETag retrieves the ETag for a specific key
func (c *StateStore) getETag(ctx context.Context, key string) (string, error) {
	query := fmt.Sprintf(`
		SELECT etag 
		FROM %s.%s FINAL  -- Add FINAL to get the latest version
		WHERE key = ? AND (expire IS NULL OR expire > now64())
	`, c.config.Database, c.config.Table)
	
	var etag string
	err := c.db.QueryRowContext(ctx, query, key).Scan(&etag)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("error getting etag: %v", err)
	}
	
	return etag, nil
}

// keyExists checks if a key exists in the state store
func (c *StateStore) keyExists(ctx context.Context, key string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT 1 
		FROM %s.%s FINAL  -- Add FINAL to get the latest version
		WHERE key = ? AND (expire IS NULL OR expire > now64())
		LIMIT 1
	`, c.config.Database, c.config.Table)
	
	var exists int
	err := c.db.QueryRowContext(ctx, query, key).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("error checking key existence: %v", err)
	}
	
	return true, nil
}
