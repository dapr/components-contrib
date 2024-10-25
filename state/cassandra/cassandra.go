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

package cassandra

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	hosts                    = "hosts"
	port                     = "port"
	username                 = "username"
	password                 = "password"
	protoVersion             = "protoVersion"
	consistency              = "consistency"
	table                    = "table"
	keyspace                 = "keyspace"
	replicationFactor        = "replicationFactor"
	defaultProtoVersion      = 4
	defaultReplicationFactor = 1
	defaultConsistency       = gocql.All
	defaultTable             = "items"
	defaultKeyspace          = "dapr"
	defaultPort              = 9042
	metadataTTLKey           = "ttlInSeconds"
)

// Cassandra is a state store implementation for Apache Cassandra.
type Cassandra struct {
	state.BulkStore

	session *gocql.Session
	cluster *gocql.ClusterConfig
	table   string

	logger logger.Logger
}

type cassandraMetadata struct {
	Hosts                  []string
	Port                   int
	ProtoVersion           int
	ReplicationFactor      int
	Username               string
	Password               string
	Consistency            string
	Table                  string
	Keyspace               string
	EnableHostVerification bool
}

// NewCassandraStateStore returns a new cassandra state store.
func NewCassandraStateStore(logger logger.Logger) state.Store {
	s := &Cassandra{
		logger: logger,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// Init performs metadata and connection parsing.
func (c *Cassandra) Init(_ context.Context, metadata state.Metadata) error {
	meta, err := getCassandraMetadata(metadata)
	if err != nil {
		return err
	}

	cluster, err := c.createClusterConfig(meta)
	if err != nil {
		return fmt.Errorf("error creating cluster config: %w", err)
	}
	c.cluster = cluster

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("error creating session: %w", err)
	}
	c.session = session

	err = c.tryCreateKeyspace(meta.Keyspace, meta.ReplicationFactor)
	if err != nil {
		return fmt.Errorf("error creating keyspace %s: %w", meta.Keyspace, err)
	}

	err = c.tryCreateTable(meta.Table, meta.Keyspace)
	if err != nil {
		return fmt.Errorf("error creating table %s: %w", meta.Table, err)
	}

	c.table = meta.Keyspace + "." + meta.Table

	return nil
}

// Features returns the features available in this state store.
func (c *Cassandra) Features() []state.Feature {
	return []state.Feature{
		state.FeatureTTL,
	}
}

func (c *Cassandra) tryCreateKeyspace(keyspace string, replicationFactor int) error {
	return c.session.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : %s};", keyspace, strconv.Itoa(replicationFactor))).Exec()
}

func (c *Cassandra) tryCreateTable(table, keyspace string) error {
	return c.session.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (key text, value blob, PRIMARY KEY (key));", keyspace, table)).Exec()
}

func (c *Cassandra) createClusterConfig(metadata *cassandraMetadata) (*gocql.ClusterConfig, error) {
	clusterConfig := gocql.NewCluster(metadata.Hosts...)
	if metadata.Username != "" && metadata.Password != "" {
		clusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: metadata.Username, Password: metadata.Password}
	}
	if metadata.EnableHostVerification {
		clusterConfig.SslOpts = &gocql.SslOptions{
			EnableHostVerification: true,
		}
	}
	clusterConfig.Port = metadata.Port
	clusterConfig.ProtoVersion = metadata.ProtoVersion
	cons, err := c.getConsistency(metadata.Consistency)
	if err != nil {
		return nil, err
	}

	clusterConfig.Consistency = cons

	return clusterConfig, nil
}

func (c *Cassandra) getConsistency(consistency string) (gocql.Consistency, error) {
	switch consistency {
	case "All":
		return gocql.All, nil
	case "One":
		return gocql.One, nil
	case "Two":
		return gocql.Two, nil
	case "Three":
		return gocql.Three, nil
	case "Quorum":
		return gocql.Quorum, nil
	case "LocalQuorum":
		return gocql.LocalQuorum, nil
	case "EachQuorum":
		return gocql.EachQuorum, nil
	case "LocalOne":
		return gocql.LocalOne, nil
	case "Any":
		return gocql.Any, nil
	case "":
		return defaultConsistency, nil
	}

	return 0, fmt.Errorf("consistency mode %s not found", consistency)
}

func getCassandraMetadata(meta state.Metadata) (*cassandraMetadata, error) {
	m := cassandraMetadata{
		ProtoVersion:      defaultProtoVersion,
		Table:             defaultTable,
		Keyspace:          defaultKeyspace,
		ReplicationFactor: defaultReplicationFactor,
		Consistency:       "All",
		Port:              defaultPort,
	}
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	if m.Hosts == nil || len(m.Hosts) == 0 {
		return nil, errors.New("missing or empty hosts field from metadata")
	}

	if val, ok := meta.Properties[port]; ok && val != "" {
		p, err := strconv.ParseInt(val, 0, 32)
		if err != nil {
			return nil, fmt.Errorf("error parsing port field: %s", err)
		}
		m.Port = int(p)
	}

	if val, ok := meta.Properties[protoVersion]; ok && val != "" {
		p, err := strconv.ParseInt(val, 0, 32)
		if err != nil {
			return nil, fmt.Errorf("error parsing protoVersion field: %s", err)
		}
		m.ProtoVersion = int(p)
	}

	if val, ok := meta.Properties[replicationFactor]; ok && val != "" {
		r, err := strconv.ParseInt(val, 0, 32)
		if err != nil {
			return nil, fmt.Errorf("error parsing replicationFactor field: %s", err)
		}
		m.ReplicationFactor = int(r)
	}

	return &m, nil
}

// Delete performs a delete operation.
func (c *Cassandra) Delete(ctx context.Context, req *state.DeleteRequest) error {
	return c.session.Query(fmt.Sprintf("DELETE FROM %s WHERE key = ?", c.table), req.Key).WithContext(ctx).Exec()
}

// Get retrieves state from cassandra with a key.
func (c *Cassandra) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	session := c.session

	if req.Options.Consistency == state.Strong {
		sess, err := c.createSession(gocql.All)
		if err != nil {
			return nil, err
		}
		defer sess.Close()
		session = sess
	} else if req.Options.Consistency == state.Eventual {
		sess, err := c.createSession(gocql.One)
		if err != nil {
			return nil, err
		}
		defer sess.Close()
		session = sess
	}

	const selectQuery = "SELECT value, TTL(value) AS ttl, toTimestamp(now()) AS now FROM %s WHERE key = ?"
	results, err := session.Query(fmt.Sprintf(selectQuery, c.table), req.Key).WithContext(ctx).Iter().SliceMap()
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return &state.GetResponse{}, nil
	}

	var metadata map[string]string
	if ttl := results[0]["ttl"].(int); ttl > 0 {
		now, ok := results[0]["now"].(time.Time)
		if !ok {
			return nil, errors.New("failed to parse cassandra timestamp")
		}
		metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: now.Add(time.Duration(ttl) * time.Second).UTC().Format(time.RFC3339),
		}
	}

	return &state.GetResponse{
		Data:     results[0]["value"].([]byte),
		Metadata: metadata,
	}, nil
}

// Set saves state into cassandra.
func (c *Cassandra) Set(ctx context.Context, req *state.SetRequest) error {
	var bt []byte
	b, ok := req.Value.([]byte)
	if ok {
		bt = b
	} else {
		bt, _ = jsoniter.ConfigFastest.Marshal(req.Value)
	}

	session := c.session

	if req.Options.Consistency == state.Strong {
		sess, err := c.createSession(gocql.Quorum)
		if err != nil {
			return err
		}
		defer sess.Close()
		session = sess
	} else if req.Options.Consistency == state.Eventual {
		sess, err := c.createSession(gocql.Any)
		if err != nil {
			return err
		}
		defer sess.Close()
		session = sess
	}

	ttl, err := stateutils.ParseTTL(req.Metadata)
	if err != nil {
		return fmt.Errorf("error parsing TTL from Metadata: %s", err)
	}

	if ttl != nil {
		return session.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?) USING TTL ?", c.table), req.Key, bt, *ttl).WithContext(ctx).Exec()
	}

	return session.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", c.table), req.Key, bt).WithContext(ctx).Exec()
}

func (c *Cassandra) createSession(consistency gocql.Consistency) (*gocql.Session, error) {
	session, err := c.cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("error creating session: %s", err)
	}

	session.SetConsistency(consistency)

	return session, nil
}

func (c *Cassandra) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := cassandraMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

// Close the connection to Cassandra.
func (c *Cassandra) Close() error {
	if c.session == nil {
		return nil
	}

	c.session.Close()

	return nil
}
