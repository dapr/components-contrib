package immudb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

type ImmudbStateStore struct {
	state.BulkStore

	client immudb.ImmuClient
	logger logger.Logger
}

type immudbMetadata struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

func NewImmudbStateStore(logger logger.Logger) state.Store {
	s := &ImmudbStateStore{
		logger: logger,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

func (i *ImmudbStateStore) Init(ctx context.Context, metadata state.Metadata) error {
	meta, err := getImmudbMetadata(metadata)
	if err != nil {
		return err
	}

	opts := immudb.DefaultOptions().
		WithAddress(meta.Host).
		WithPort(meta.Port)

	client := immudb.NewClient().WithOptions(opts)
	err = client.OpenSession(
		ctx,
		[]byte(meta.Username),
		[]byte(meta.Password),
		meta.Database,
	)
	if err != nil {
		i.logger.Debug("failed to open session: %v", err)
		return fmt.Errorf("failed to open session: %v", err)
	}

	i.client = client
	return nil
}

func (i *ImmudbStateStore) Features() []state.Feature {
	return nil
}

func (i *ImmudbStateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	encodedKey := i.encodeKey(req.Key)
	i.logger.Debugf("Getting value for encoded key: %s", encodedKey)

	entry, err := i.client.VerifiedGet(ctx, []byte(encodedKey))
	if err != nil {
		if strings.Contains(err.Error(), "key not found") {
			return &state.GetResponse{}, nil
		}

		i.logger.Debugf("Failed to get value: %v", err)
		return nil, fmt.Errorf("failed to get value for key %s: %w", req.Key, err)
	}

	i.logger.Debugf("Successfully retrieved value for key: %s", req.Key)
	return &state.GetResponse{
		Data: entry.Value,
	}, nil
}

func (i *ImmudbStateStore) Set(ctx context.Context, req *state.SetRequest) error {
	var b []byte
	var err error
	i.logger.Debugf("Set Request - Key: %s, Value Type: %T", req.Key, req.Value)

	encodedKey := i.encodeKey(req.Key)
	i.logger.Debugf("Encoded key for Immudb: %s", encodedKey)

	if byteSlice, ok := req.Value.([]byte); ok {
		// Special handling for []byte to avoid unnecessary encoding
		b = byteSlice

		i.logger.Debugf("Value is []byte: %s", hex.EncodeToString(b))
	} else {
		// Use json.Marshal for all other types
		b, err = json.Marshal(req.Value)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %v", err)
		}

		i.logger.Debugf("Marshaled JSON value: %s", string(b))
	}

	_, err = i.client.VerifiedSet(ctx, []byte(encodedKey), b)
	if err != nil {
		i.logger.Errorf("Failed to set value: %v", err)
		return fmt.Errorf("failed to set value: %v", err)
	}

	i.logger.Debugf("Successfully set: original key %s, Immudb key %s, value type %T", req.Key, encodedKey, req.Value)
	return nil
}

func (i *ImmudbStateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	encodedKey := i.encodeKey(req.Key)
	i.logger.Debugf("Deleting key: %s", encodedKey)

	_, err := i.client.Delete(ctx, &schema.DeleteKeysRequest{
		Keys: [][]byte{
			[]byte(encodedKey),
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "key not found") {
			i.logger.Debugf("Key not found, nothing to delete: %s", req.Key)
			return nil
		}

		i.logger.Debugf("Failed to delete key: %v", err)
		return fmt.Errorf("failed to delete key: %v", err)
	}

	i.logger.Debugf("Successfully deleted key: %s", req.Key)
	return nil
}

func (i *ImmudbStateStore) Close() error {
	i.logger.Debug("Closing Immudb session")
	if i.client == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	return i.client.CloseSession(ctx)
}

func (i *ImmudbStateStore) Ping(ctx context.Context) error {
	if i.client == nil {
		return fmt.Errorf("client is not initialized")
	}

	err := i.client.HealthCheck(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping ImmuDB: %v", err)
	}
	return nil
}

func (i *ImmudbStateStore) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := immudbMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func getImmudbMetadata(meta state.Metadata) (*immudbMetadata, error) {
	m := immudbMetadata{}
	if err := kitmd.DecodeMetadata(meta.Properties, &m); err != nil {
		return nil, err
	}

	if m.Host == "" {
		return nil, fmt.Errorf("host is required")
	}
	if m.Port == 0 {
		m.Port = 3322 // Default ImmuDB port
	}
	if m.Username == "" {
		return nil, fmt.Errorf("username is required")
	}
	if m.Password == "" {
		return nil, fmt.Errorf("password is required")
	}
	if m.Database == "" {
		m.Database = "defaultdb"
	}

	return &m, nil
}

// encodeKey convert the key to a format that can be stored in ImmuDB, prefix || added by Dapr isn't allowed
func (i *ImmudbStateStore) encodeKey(key string) string {
	// Reemplaza "||" con "~~"
	encoded := strings.ReplaceAll(key, "||", "~~")
	i.logger.Debugf("Encoded key '%s' to '%s'", key, encoded)
	return encoded
}

// decodeKey convert the key from the format stored in ImmuDB to the original key
func (i *ImmudbStateStore) decodeKey(encodedKey string) string {
	// Reemplaza "~~" con "||"
	decoded := strings.ReplaceAll(encodedKey, "~~", "||")
	i.logger.Debugf("Decoded key '%s' to '%s'", encodedKey, decoded)
	return decoded
}
