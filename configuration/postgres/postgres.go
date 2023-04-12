/*
Copyright 2022 The Dapr Authors
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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/utils/strings/slices"

	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type ConfigurationStore struct {
	metadata             metadata
	client               *pgxpool.Pool
	logger               logger.Logger
	configLock           sync.Mutex
	subscribeStopChanMap map[string]chan struct{}
	ActiveSubscriptions  map[string]*subscription
}

type subscription struct {
	channel string
	keys    []string
}

type pgResponse struct {
	key  string
	item *configuration.Item
}

const (
	configtablekey               = "table"
	connMaxIdleTimeKey           = "connMaxIdleTime"
	connectionStringKey          = "connectionString"
	pgNotifyChannelKey           = "pgNotifyChannel"
	listenTemplate               = "listen %s"
	unlistenTemplate             = "unlisten %s"
	payloadDataKey               = "data"
	ErrorMissingTableName        = "missing postgreSQL configuration table name"
	ErrorMissingTable            = "postgreSQL configuration table - '%s' does not exist"
	InfoStartInit                = "initializing postgreSQL configuration store"
	ErrorMissingConnectionString = "missing postgreSQL connection string"
	ErrorAlreadyInitialized      = "postgreSQL configuration store already initialized"
	ErrorMissingMaxTimeout       = "missing PostgreSQL maxTimeout setting in configuration"
	QueryTableExists             = "SELECT EXISTS (SELECT FROM pg_tables where tablename = $1)"
	ErrorTooLongFieldLength      = "field name is too long"
	maxIdentifierLength          = 64 // https://www.postgresql.org/docs/current/limits.html
)

var (
	allowedChars           = regexp.MustCompile(`^[a-zA-Z0-9./_]*$`)
	allowedTableNameChars  = regexp.MustCompile(`^[a-z0-9./_]*$`)
	defaultMaxConnIdleTime = time.Second * 30
)

func NewPostgresConfigurationStore(logger logger.Logger) configuration.Store {
	logger.Debug("Instantiating PostgreSQL configuration store")
	return &ConfigurationStore{
		logger:               logger,
		subscribeStopChanMap: make(map[string]chan struct{}),
	}
}

func (p *ConfigurationStore) Init(parentCtx context.Context, metadata configuration.Metadata) error {
	p.logger.Debug(InfoStartInit)
	if p.client != nil {
		return fmt.Errorf(ErrorAlreadyInitialized)
	}
	if m, err := parseMetadata(metadata); err != nil {
		p.logger.Error(err)
		return err
	} else {
		p.metadata = m
	}
	p.ActiveSubscriptions = make(map[string]*subscription)
	ctx, cancel := context.WithTimeout(parentCtx, p.metadata.MaxIdleTimeout)
	defer cancel()
	client, err := Connect(ctx, p.metadata.ConnectionString, p.metadata.MaxIdleTimeout)
	if err != nil {
		return fmt.Errorf("error connecting to configuration store: '%w'", err)
	}
	p.client = client
	err = p.client.Ping(ctx)
	if err != nil {
		return fmt.Errorf("unable to connect to configuration store: '%w'", err)
	}
	// check if table exists
	exists := false
	err = p.client.QueryRow(ctx, QueryTableExists, p.metadata.ConfigTable).Scan(&exists)
	if err != nil {
		return fmt.Errorf("error in checking if configtable '%s' exists - '%w'", p.metadata.ConfigTable, err)
	}
	if !exists {
		return fmt.Errorf(ErrorMissingTable, p.metadata.ConfigTable)
	}
	return nil
}

func (p *ConfigurationStore) Get(ctx context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	if err := validateInput(req.Keys); err != nil {
		p.logger.Error(err)
		return nil, err
	}
	query, params, err := buildQuery(req, p.metadata.ConfigTable)
	if err != nil {
		p.logger.Error(err)
		return nil, fmt.Errorf("error in configuration store query: '%w' ", err)
	}
	rows, err := p.client.Query(ctx, query, params...)
	if err != nil {
		// If no rows exist, return an empty response, otherwise return the error.
		if errors.Is(err, pgx.ErrNoRows) {
			return &configuration.GetResponse{}, nil
		}
		return nil, fmt.Errorf("error in querying configuration store: '%w'", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (pgResponse, error) {
		res := pgResponse{
			item: new(configuration.Item),
		}
		if innerErr := row.Scan(&res.key, &res.item.Value, &res.item.Version, &res.item.Metadata); innerErr != nil {
			return pgResponse{}, fmt.Errorf("error in reading data from configuration store: '%w'", innerErr)
		}
		return res, nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to parse response from configuration store - %w", err)
	}
	result := make(map[string]*configuration.Item)
	for _, v := range items {
		result[v.key] = v.item
	}
	return &configuration.GetResponse{
		Items: result,
	}, nil
}

func (p *ConfigurationStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) (string, error) {
	pgNotifyChannel := ""
	for k, v := range req.Metadata {
		if strings.EqualFold(pgNotifyChannelKey, k) {
			pgNotifyChannel = v
		}
	}
	if pgNotifyChannel == "" {
		return "", fmt.Errorf("unable to subscribe to '%s'.pgNotifyChannel attribute cannot be empty", p.metadata.ConfigTable)
	}
	return p.subscribeToChannel(ctx, pgNotifyChannel, req, handler)
}

func (p *ConfigurationStore) Unsubscribe(ctx context.Context, req *configuration.UnsubscribeRequest) error {
	p.configLock.Lock()
	defer p.configLock.Unlock()
	if sub, ok := p.ActiveSubscriptions[req.ID]; ok {
		if oldStopChan, ok := p.subscribeStopChanMap[req.ID]; ok {
			delete(p.subscribeStopChanMap, req.ID)
			close(oldStopChan)
		}
		pgChannel := fmt.Sprintf(unlistenTemplate, sub.channel)
		conn, err := p.client.Acquire(ctx)
		if err != nil {
			p.logger.Errorf("error acquiring connection:", err)
			return fmt.Errorf("error acquiring connection: %w ", err)
		}
		defer conn.Release()
		_, err = conn.Exec(ctx, pgChannel)
		if err != nil {
			p.logger.Errorf("error un-listening to channel:", err)
			return fmt.Errorf("error un-listening to channel: %w", err)
		}
		delete(p.ActiveSubscriptions, req.ID)
		return nil
	}

	return fmt.Errorf("unable to find subscription with ID : %v", req.ID)
}

func (p *ConfigurationStore) doSubscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler, command string, channel string, subscription string, stop chan struct{}) {
	conn, err := p.client.Acquire(ctx)
	if err != nil {
		p.logger.Errorf("error acquiring connection:", err)
		return
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, command); err != nil {
		p.logger.Errorf("error listening to channel:", err)
		return
	}
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if !pgconn.Timeout(err) && !errors.Is(err, context.Canceled) {
				p.logger.Errorf("error waiting for notification:", err)
			}
			return
		}
		p.handleSubscribedChange(ctx, handler, notification, channel, subscription)
	}
}

func (p *ConfigurationStore) handleSubscribedChange(ctx context.Context, handler configuration.UpdateHandler, msg *pgconn.Notification, channel string, subscriptionID string) {
	payload := make(map[string]interface{})
	err := json.Unmarshal([]byte(msg.Payload), &payload)
	if err != nil {
		p.logger.Errorf("error in unmarshal: ", err)
		return
	}
	var key, value, version string
	m := make(map[string]string)
	// trigger should encapsulate the row in "data" field in the notification
	row := reflect.ValueOf(payload[payloadDataKey])
	if row.Kind() == reflect.Map {
		for _, k := range row.MapKeys() {
			v := row.MapIndex(k)
			strKey := k.Interface().(string)
			switch strings.ToLower(strKey) {
			case "key":
				key = v.Interface().(string)
				if yes := p.isSubscribed(subscriptionID, channel, key); !yes {
					p.logger.Debugf("ignoring notification for %v", key)
					return
				}
			case "value":
				value = v.Interface().(string)
			case "version":
				version = v.Interface().(string)
			case "metadata":
				if v.Interface() == nil {
					continue
				}
				a := v.Interface().(map[string]interface{})
				for k, v := range a {
					m[k] = v.(string)
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
			ID: subscriptionID,
		}
		err = handler(ctx, e)
		if err != nil {
			p.logger.Errorf("failed to call notify event handler : %w", err)
		}
	} else {
		p.logger.Info("unknown format of data received in notify event - '%s'", msg.Payload)
	}
}

func parseMetadata(cmetadata configuration.Metadata) (metadata, error) {
	m := metadata{
		MaxIdleTimeout: defaultMaxConnIdleTime,
	}
	decodeErr := contribMetadata.DecodeMetadata(cmetadata.Properties, &m)
	if decodeErr != nil {
		return m, decodeErr
	}

	if m.ConnectionString == "" {
		return m, fmt.Errorf(ErrorMissingConnectionString)
	}

	if m.ConfigTable != "" {
		if !allowedTableNameChars.MatchString(m.ConfigTable) {
			return m, fmt.Errorf("invalid table name : '%v'. non-alphanumerics or upper cased table names are not supported", m.ConfigTable)
		}
		if len(m.ConfigTable) > maxIdentifierLength {
			return m, fmt.Errorf(ErrorTooLongFieldLength+" - tableName : '%v'. max allowed field length is %v ", m.ConfigTable, maxIdentifierLength)
		}
	} else {
		return m, fmt.Errorf(ErrorMissingTableName)
	}
	if m.MaxIdleTimeout <= 0 {
		m.MaxIdleTimeout = defaultMaxConnIdleTime
	}
	return m, nil
}

func Connect(ctx context.Context, conn string, maxTimeout time.Duration) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(conn)
	if err != nil {
		return nil, fmt.Errorf("postgres configuration store connection error : %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("postgres configuration store connection error : %w", err)
	}
	err = pool.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres configuration store ping error : %w", err)
	}
	return pool, nil
}

func buildQuery(req *configuration.GetRequest, configTable string) (string, []interface{}, error) {
	var query string
	var params []interface{}
	if len(req.Keys) == 0 {
		query = "SELECT * FROM " + configTable
	} else {
		var queryBuilder strings.Builder
		queryBuilder.WriteString("SELECT * FROM " + configTable + " WHERE KEY IN (")
		var paramWildcard []string
		paramPosition := 1
		for _, v := range req.Keys {
			paramWildcard = append(paramWildcard, "$"+strconv.Itoa(paramPosition))
			params = append(params, v)
			paramPosition++
		}
		queryBuilder.WriteString(strings.Join(paramWildcard, " , "))
		queryBuilder.WriteString(")")
		query = queryBuilder.String()

		if len(req.Metadata) > 0 {
			var s strings.Builder
			i, j := len(req.Metadata), 0
			s.WriteString(" AND ")
			for k, v := range req.Metadata {
				temp := "$" + strconv.Itoa(paramPosition) + " = " + "$" + strconv.Itoa(paramPosition+1)
				s.WriteString(temp)
				params = append(params, k, v)
				paramPosition += 2
				if j++; j < i {
					s.WriteString(" AND ")
				}
			}
			query += s.String()
		}
	}
	return query, params, nil
}

func (p *ConfigurationStore) isSubscribed(subscriptionID string, channel string, key string) bool {
	if val, yes := p.ActiveSubscriptions[subscriptionID]; yes && val.channel == channel && (slices.Contains(val.keys, key) || len(val.keys) == 0) {
		return true
	}
	return false
}

func validateInput(keys []string) error {
	for _, key := range keys {
		if !allowedChars.MatchString(key) {
			return fmt.Errorf("invalid key : '%v'", key)
		}
	}
	return nil
}

func (p *ConfigurationStore) subscribeToChannel(ctx context.Context, pgNotifyChannel string, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) (string, error) {
	p.configLock.Lock()
	defer p.configLock.Unlock()
	var subscribeID string
	pgNotifyCmd := fmt.Sprintf(listenTemplate, pgNotifyChannel)
	stop := make(chan struct{})
	subscribeUID, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("unable to generate subscription id - %w", err)
	}
	subscribeID = subscribeUID.String()
	p.subscribeStopChanMap[subscribeID] = stop
	p.ActiveSubscriptions[subscribeID] = &subscription{
		channel: pgNotifyChannel,
		keys:    req.Keys,
	}
	go p.doSubscribe(ctx, req, handler, pgNotifyCmd, pgNotifyChannel, subscribeID, stop)
	return subscribeID, nil
}

// GetComponentMetadata returns the metadata of the component.
func (p *ConfigurationStore) GetComponentMetadata() map[string]string {
	metadataStruct := metadata{}
	metadataInfo := map[string]string{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.ConfigurationStoreType)
	return metadataInfo
}
