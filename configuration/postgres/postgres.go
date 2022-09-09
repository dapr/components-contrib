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

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/kit/logger"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
)

type PostgresConfigStore struct {
	metadata             metadata
	pool                 *pgxpool.Pool
	logger               logger.Logger
	subscribeStopChanMap sync.Map
}

const (
	configtablekey               = "table"
	connMaxIdleTimeKey           = "connMaxIdleTime"
	connectionStringKey          = "connectionString"
	ErrorMissingTableName        = "missing postgreSQL configuration table name"
	InfoStartInit                = "Initializing PostgreSQL state store"
	ErrorMissingConnectionString = "missing postgreSQL connection string"
	ErrorAlreadyInitialized      = "PostgreSQL configuration store already initialized"
	ErrorMissinMaxTimeout        = "missing PostgreSQL maxTimeout setting in configuration"
	QueryTableExists             = "SELECT EXISTS (SELECT FROM pg_tables where tablename = $1)"
)

func NewPostgresConfigurationStore(logger logger.Logger) *PostgresConfigStore {
	logger.Debug("Instantiating PostgreSQL configuration store")
	return &PostgresConfigStore{
		logger: logger,
	}
}

func (p *PostgresConfigStore) Init(metadata configuration.Metadata) error {
	p.logger.Debug(InfoStartInit)
	if p.pool != nil {
		return fmt.Errorf(ErrorAlreadyInitialized)
	}
	if m, err := parseMetadata(metadata); err != nil {
		p.logger.Error(err)
		return err
	} else {
		p.metadata = m
	}

	ctx := context.Background()
	pool, err := Connect(ctx, p.metadata.connectionString)
	if err != nil {
		return err
	}
	p.pool = pool
	pingErr := p.pool.Ping(ctx)
	if pingErr != nil {
		return pingErr
	}

	// check if table exists
	exists := false
	err = p.pool.QueryRow(ctx, QueryTableExists, p.metadata.configTable).Scan(&exists)
	if err != nil {
		return err
	}
	return nil
}

func (p *PostgresConfigStore) Get(ctx context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	query, err := buildQuery(req, p.metadata.configTable)
	if err != nil {
		p.logger.Error(err)
		return nil, err
	}

	rows, err := p.pool.Query(ctx, query)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if err == sql.ErrNoRows {
			return &configuration.GetResponse{}, nil
		}
		return nil, err
	}
	response := configuration.GetResponse{}
	for i := 0; rows.Next(); i++ {
		var item configuration.Item
		var key string
		var metadata []byte
		var v = make(map[string]string)

		if err := rows.Scan(key, &item.Value, &item.Version, &metadata); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(metadata, &v); err != nil {
			return nil, err
		}
		item.Metadata = v
		response.Items[key] = &item
	}
	return &response, nil
}

func (p *PostgresConfigStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) (string, error) {
	subscribeID := uuid.New().String()
	key := "listen " + p.metadata.configTable
	// subscribe to events raised on the configTable
	if oldStopChan, ok := p.subscribeStopChanMap.Load(key); ok {
		close(oldStopChan.(chan struct{}))
	}
	stop := make(chan struct{})
	p.subscribeStopChanMap.Store(subscribeID, stop)
	go p.doSubscribe(ctx, req, handler, key, subscribeID, stop)
	return subscribeID, nil
}

func (p *PostgresConfigStore) Unsubscribe(ctx context.Context, req *configuration.UnsubscribeRequest) error {
	if oldStopChan, ok := p.subscribeStopChanMap.Load(req.ID); ok {
		p.subscribeStopChanMap.Delete(req.ID)
		close(oldStopChan.(chan struct{}))
	}
	return nil
}

func (p *PostgresConfigStore) doSubscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler, channel string, id string, stop chan struct{}) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error acquiring connection:", err)
	}
	defer conn.Release()

	_, err = conn.Exec(context.Background(), channel)
	if err != nil {
		p.logger.Errorf("Error listening to channel:", err)
		return
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			p.logger.Errorf("Error waiting for notification:", err)
			return
		}
		p.handleSubscribedChange(ctx, handler, notification, id)
	}
}

func (p *PostgresConfigStore) handleSubscribedChange(ctx context.Context, handler configuration.UpdateHandler, msg *pgconn.Notification, id string) {
	defer func() {
		if err := recover(); err != nil {
			p.logger.Errorf("panic in handleSubscribedChange(）method and recovered: %s", err)
		}
	}()
	payload := make(map[string]interface{})
	err := json.Unmarshal([]byte(msg.Payload), &payload)
	if err != nil {
		p.logger.Errorf("Error in UnMarshall: ", err)
		return
	}

	var key, value, version string
	m := make(map[string]string)
	// trigger should encapsulate the row in "data" field in the notification
	row := reflect.ValueOf(payload["data"])
	if row.Kind() == reflect.Map {
		for _, k := range row.MapKeys() {
			v := row.MapIndex(k)
			strKey := k.Interface().(string)
			switch strings.ToLower(strKey) {
			case "key":
				key = v.Interface().(string)
			case "value":
				value = v.Interface().(string)
			case "version":
				version = v.Interface().(string)
			case "metadata":
				a := v.Interface().(map[string]interface{})
				for k, v := range a {
					m[k] = v.(string)
				}
			}
		}
	}
	e := &configuration.UpdateEvent{
		Items: map[string]*configuration.Item{
			key: {
				Value:    value,
				Version:  version,
				Metadata: m,
			},
		},
		ID: id,
	}
	err = handler(ctx, e)
	if err != nil {
		p.logger.Errorf("fail to call handler to notify event for configuration update subscribe: %s", err)
	}
}

func parseMetadata(cmetadata configuration.Metadata) (metadata, error) {
	m := metadata{}

	if val, ok := cmetadata.Properties[connectionStringKey]; ok && val != "" {
		m.connectionString = val
	} else {
		return m, fmt.Errorf(ErrorMissingConnectionString)
	}

	if tbl, ok := cmetadata.Properties[configtablekey]; ok && tbl != "" {
		m.configTable = tbl
	} else {
		return m, fmt.Errorf(ErrorMissingTableName)
	}

	// configure maxTimeout if provided
	if mxTimeout, ok := cmetadata.Properties[connMaxIdleTimeKey]; ok && mxTimeout != "" {
		if t, err := time.ParseDuration(mxTimeout); err == nil {
			m.maxIdleTime = t
		} else {
			return m, fmt.Errorf(ErrorMissinMaxTimeout)
		}
	}
	return m, nil
}

func Connect(ctx context.Context, conn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.Connect(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("postgres configuration store connection error : %s", err)
	}
	pingErr := pool.Ping(context.Background())
	if pingErr != nil {
		return nil, fmt.Errorf("postgres configuration store ping error : %s", pingErr)
	}
	return pool, nil
}

func buildQuery(req *configuration.GetRequest, configTable string) (string, error) {
	var query string
	if len(req.Keys) == 0 {
		query = "SELECT * FROM " + configTable
	} else {
		var queryBuilder strings.Builder
		queryBuilder.WriteString("SELECT * FROM " + configTable + " WHERE KEY IN ('")
		queryBuilder.WriteString(strings.Join(req.Keys, "','"))
		queryBuilder.WriteString("')")
		query = queryBuilder.String()
	}

	if len(req.Metadata) > 0 {
		var s strings.Builder
		i, j := len(req.Metadata), 0
		s.WriteString(" AND ")
		for k, v := range req.Metadata {
			temp := k + "='" + v + "'"
			s.WriteString(temp)
			if j++; j < i {
				s.WriteString(" AND ")
			}
		}
		query += s.String()
	}
	return query, nil
}

func QueryRow(ctx context.Context, p *pgxpool.Pool, query string, tbl string) error {
	exists := false
	err := p.QueryRow(ctx, query, tbl).Scan(&exists)
	if err != nil {
		return fmt.Errorf("postgres configuration store query error : %s", err)
	}
	return nil
}
