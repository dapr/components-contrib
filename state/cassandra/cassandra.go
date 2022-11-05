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
	"fmt"
	"reflect"
	"strconv"

	"github.com/gocql/gocql"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
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
	state.DefaultBulkStore
	session *gocql.Session
	cluster *gocql.ClusterConfig
	table   string

	logger logger.Logger
}

type cassandraMetadata struct {
	Hosts             []string
	Port              int
	ProtoVersion      int
	ReplicationFactor int
	Username          string
	Password          string
	Consistency       string
	Table             string
	Keyspace          string
}

// NewCassandraStateStore returns a new cassandra state store.
func NewCassandraStateStore(logger logger.Logger) state.Store {
	s := &Cassandra{logger: logger}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init performs metadata and connection parsing.
func (c *Cassandra) Init(metadata state.Metadata) error {
	meta, err := getCassandraMetadata(metadata)
	if err != nil {
		return err
	}

	cluster, err := c.createClusterConfig(meta)
	if err != nil {
		return fmt.Errorf("error creating cluster config: %s", err)
	}
	c.cluster = cluster

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("error creating session: %s", err)
	}
	c.session = session

	err = c.tryCreateKeyspace(meta.Keyspace, meta.ReplicationFactor)
	if err != nil {
		return fmt.Errorf("error creating keyspace %s: %s", meta.Keyspace, err)
	}

	err = c.tryCreateTable(meta.Table, meta.Keyspace)
	if err != nil {
		return fmt.Errorf("error creating table %s: %s", meta.Table, err)
	}

	c.table = fmt.Sprintf("%s.%s", meta.Keyspace, meta.Table)

	return nil
}

// Features returns the features available in this state store.
func (c *Cassandra) Features() []state.Feature {
	return nil
}

func (c *Cassandra) tryCreateKeyspace(keyspace string, replicationFactor int) error {
	return c.session.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : %s};", keyspace, fmt.Sprintf("%v", replicationFactor))).Exec()
}

func (c *Cassandra) tryCreateTable(table, keyspace string) error {
	return c.session.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (key text, value blob, PRIMARY KEY (key));", keyspace, table)).Exec()
}

func (c *Cassandra) createClusterConfig(metadata *cassandraMetadata) (*gocql.ClusterConfig, error) {
	clusterConfig := gocql.NewCluster(metadata.Hosts...)
	if metadata.Username != "" && metadata.Password != "" {
		clusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: metadata.Username, Password: metadata.Password}
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
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	if m.Hosts == nil || len(m.Hosts) == 0 {
		return nil, fmt.Errorf("missing or empty hosts field from metadata")
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
func (c *Cassandra) Delete(req *state.DeleteRequest) error {
	return c.session.Query(fmt.Sprintf("DELETE FROM %s WHERE key = ?", c.table), req.Key).Exec()
}

// Get retrieves state from cassandra with a key.
func (c *Cassandra) Get(req *state.GetRequest) (*state.GetResponse, error) {
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

	results, err := session.Query(fmt.Sprintf("SELECT value FROM %s WHERE key = ?", c.table), req.Key).Iter().SliceMap()
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return &state.GetResponse{}, nil
	}

	return &state.GetResponse{
		Data: results[0]["value"].([]byte),
	}, nil
}

// Set saves state into cassandra.
func (c *Cassandra) Set(req *state.SetRequest) error {
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

	ttl, err := parseTTL(req.Metadata)
	if err != nil {
		return fmt.Errorf("error parsing TTL from Metadata: %s", err)
	}

	if ttl != nil {
		return session.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?) USING TTL ?", c.table), req.Key, bt, *ttl).Exec()
	}

	return session.Query(fmt.Sprintf("INSERT INTO %s (key, value) VALUES (?, ?)", c.table), req.Key, bt).Exec()
}

func (c *Cassandra) createSession(consistency gocql.Consistency) (*gocql.Session, error) {
	session, err := c.cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("error creating session: %s", err)
	}

	session.SetConsistency(consistency)

	return session, nil
}

func parseTTL(requestMetadata map[string]string) (*int, error) {
	if val, found := requestMetadata[metadataTTLKey]; found && val != "" {
		parsedVal, err := strconv.ParseInt(val, 10, 0)
		if err != nil {
			return nil, err
		}
		parsedInt := int(parsedVal)

		return &parsedInt, nil
	}

	return nil, nil
}

func (c *Cassandra) GetComponentMetadata() map[string]string {
	metadataStruct := cassandraMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
