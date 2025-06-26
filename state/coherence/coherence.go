/*
Copyright 2025 The Dapr Authors
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

package coherence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	coh "github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/coherence/processors"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	serverAddressConfig     = "serverAddress"
	tlsEnabledConfig        = "tlsEnabled"
	tlsClientCertPathConfig = "tlsClientCertPath"
	tlsClientKeyConfig      = "tlsClientKey"
	tlsCertsPathConfig      = "tlsCertsPath"
	ignoreInvalidCerts      = "ignoreInvalidCerts"
	requestTimeoutConfig    = "requestTimeout"
	nearCacheTTLConfig      = "nearCacheTTL"
	nearCacheUnitsConfig    = "nearCacheUnits"
	nearCacheMemoryConfig   = "nearCacheMemory"
	scopeNameConfig         = "scopeName"
	defaultScopeNameConfig  = "default"
	defaultServerAddress    = "localhost:1408"
	defaultTLSEnabled       = false
)

var (
	errTrueOrFalse        = errors.New("value should be true or false")
	defaultRequestTimeout = time.Duration(30) * time.Second
	defaultNearCacheTTL   = time.Duration(0)
)

type Coherence struct {
	state.BulkStore

	session    *coh.Session
	namedCache coh.NamedCache[string, []byte]
	logger     logger.Logger
}

type coherenceMetadata struct {
	ServerAddress      string        `json:"serverAddress"`
	TLSEnabled         bool          `json:"tlsEnabled"`
	TLSClientCertPath  string        `json:"tlsClientCertPathConfig"`
	TLSClientKey       string        `json:"tlsClientKey"`
	TLSCertsPath       string        `json:"tlsCertsPath"`
	IgnoreInvalidCerts bool          `json:"ignoreInvalidCerts"`
	RequestTimeout     time.Duration `json:"requestTimeout"`
	NearCacheTTL       time.Duration `json:"nearCacheTimeout"`
	NearCacheUnits     int64         `json:"nearCacheUnits"`
	NearCacheMemory    int64         `json:"nearCacheMemory"`
	ScopeName          string        `json:"scopeName"`
}

// NewCoherenceStateStore returns a new Coherence state store.
func NewCoherenceStateStore(logger logger.Logger) state.Store {
	c := &Coherence{logger: logger}
	c.BulkStore = state.NewDefaultBulkStore(c)
	return c
}

func (c *Coherence) Init(_ context.Context, metadata state.Metadata) error {
	meta, err := retrieveCoherenceMetadata(metadata)
	if err != nil {
		return err
	}

	options := make([]func(session *coh.SessionOptions), 0)

	options = append(options, coh.WithAddress(meta.ServerAddress))

	// configure TLS and options
	if !meta.TLSEnabled {
		options = append(options, coh.WithPlainText())
	} else {
		if meta.TLSClientCertPath != "" {
			options = append(options, coh.WithTLSClientCert(meta.TLSClientCertPath))
		}
		if meta.TLSCertsPath != "" {
			options = append(options, coh.WithTLSCertsPath(meta.TLSCertsPath))
		}
		if meta.TLSClientKey != "" {
			options = append(options, coh.WithTLSClientKey(meta.TLSClientKey))
		}
		if meta.IgnoreInvalidCerts {
			options = append(options, coh.WithIgnoreInvalidCerts())
		}
	}

	options = append(options, coh.WithRequestTimeout(meta.RequestTimeout))

	session, err := coh.NewSession(context.Background(), options...)
	if err != nil {
		return err
	}

	c.logger.Info("Created session", session)

	cacheOptions := make([]func(session *coh.CacheOptions), 0)
	// create the cache and configure a near cache if the nearCacheTimeout is set
	if meta.NearCacheTTL != 0 || meta.NearCacheUnits != 0 || meta.NearCacheMemory != 0 {
		nearCacheOptions := coh.NearCacheOptions{TTL: meta.NearCacheTTL, HighUnits: meta.NearCacheUnits, HighUnitsMemory: meta.NearCacheMemory}
		cacheOptions = append(cacheOptions, coh.WithNearCache(&nearCacheOptions))
	}

	nc, err := coh.GetNamedCache[string, []byte](session, "dapr$"+meta.ScopeName, cacheOptions...)
	if err != nil {
		return err
	}

	c.namedCache = nc
	c.session = session

	c.logger.Info("Using cache", nc.Name())

	return nil
}

func retrieveCoherenceMetadata(meta state.Metadata) (*coherenceMetadata, error) {
	c := coherenceMetadata{
		ServerAddress:  defaultServerAddress,
		TLSEnabled:     defaultTLSEnabled,
		RequestTimeout: defaultRequestTimeout,
		NearCacheTTL:   defaultNearCacheTTL,
		ScopeName:      defaultScopeNameConfig,
	}

	var (
		duration   time.Duration
		err        error
		booleanVal bool
		int64Val   int64
	)

	err = kitmd.DecodeMetadata(meta.Properties, &c)
	if err != nil {
		return nil, err
	}

	// retrieve the server address
	val := getStringMetaProperty(meta, serverAddressConfig)
	if val != "" {
		c.ServerAddress = val
	}

	// retrieve the scope name
	val = getStringMetaProperty(meta, scopeNameConfig)
	if val != "" {
		c.ScopeName = val
	}

	// retrieve the request timeout in millis
	duration, err = getDurationMetaProperty(meta, requestTimeoutConfig)
	if err != nil {
		return nil, err
	}
	if duration > 0 {
		c.RequestTimeout = duration
	}

	// retrieve the near cache timeout and values
	duration, err = getDurationMetaProperty(meta, nearCacheTTLConfig)
	if err != nil {
		return nil, err
	}
	c.NearCacheTTL = duration

	nearCacheUnits := getStringMetaProperty(meta, nearCacheUnitsConfig)
	nearCacheMemory := getStringMetaProperty(meta, nearCacheMemoryConfig)

	if nearCacheUnits != "" && nearCacheMemory != "" {
		return nil, errors.New(nearCacheUnitsConfig + " and " + nearCacheMemoryConfig + "are mutually exclusive")
	}

	if nearCacheUnits != "" {
		int64Val, err = getInt64ValueFromString(nearCacheUnits)
		if err != nil {
			return nil, err
		}
		if int64Val > 0 {
			c.NearCacheUnits = int64Val
		}
	}

	if nearCacheMemory != "" {
		int64Val, err = getInt64ValueFromString(nearCacheMemory)
		if err != nil {
			return nil, err
		}
		if int64Val > 0 {
			c.NearCacheMemory = int64Val
		}
	}

	// validate if TLS is enabled
	booleanVal, err = getBoolMetaProperty(meta, tlsEnabledConfig)
	if err != nil {
		return nil, errors.New(tlsEnabledConfig + " should be true or false")
	}
	c.TLSEnabled = booleanVal

	booleanVal, err = getBoolMetaProperty(meta, ignoreInvalidCerts)
	if err != nil {
		return nil, errors.New(ignoreInvalidCerts + " should be true or false")
	}
	c.IgnoreInvalidCerts = booleanVal

	c.TLSClientKey = getStringMetaProperty(meta, tlsClientKeyConfig)
	c.TLSCertsPath = getStringMetaProperty(meta, tlsCertsPathConfig)
	c.TLSClientCertPath = getStringMetaProperty(meta, tlsClientCertPathConfig)

	if c.TLSEnabled {
		// do extra validation
		if c.TLSClientCertPath == "" || c.TLSClientKey == "" {
			return nil, errors.New(tlsClientCertPathConfig + " and " + tlsClientCertPathConfig + " must be set when TLS is enabled")
		}
	}

	return &c, nil
}

func getStringMetaProperty(meta state.Metadata, key string) string {
	if val, ok := meta.Properties[key]; ok && val != "" {
		return val
	}
	return ""
}

func getInt64ValueFromString(value string) (int64, error) {
	result, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid value for %s: %w", value, err)
	}
	return int64(result), nil
}

func getBoolMetaProperty(meta state.Metadata, key string) (bool, error) {
	if val, ok := meta.Properties[key]; ok && val != "" {
		if val != "true" && val != "false" {
			return false, errTrueOrFalse
		}
		return val == "true", nil
	}
	return false, nil // default of "" is false
}

func getDurationMetaProperty(meta state.Metadata, key string) (time.Duration, error) {
	if val, ok := meta.Properties[key]; ok && val != "" {
		t, err := time.ParseDuration(val)
		if err != nil {
			return 0, fmt.Errorf("could not parse %s value of %s - %v", key, val, err)
		}
		return t, nil
	}
	return 0, nil
}

func (c *Coherence) BulkGet(ctx context.Context, req []state.GetRequest, _ state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	var (
		err       error
		responses = make([]state.BulkGetResponse, 0)
		foundKeys = make([]string, 0)
	)
	err = state.CheckRequestOptions(req)
	if err != nil {
		return responses, err
	}

	keys := make([]string, 0, len(req))
	for _, k := range req {
		keys = append(keys, k.Key)
	}

	ch := c.namedCache.GetAll(ctx, keys)
	for entry := range ch {
		if entry.Err != nil {
			return responses, entry.Err
		}
		responses = append(responses, state.BulkGetResponse{Key: entry.Key, Data: entry.Value})
		foundKeys = append(foundKeys, entry.Key)
	}

	// note: when a get is done and the key is not there, we should have an entry with a nil value
	foundKeySet := make(map[string]bool, len(foundKeys))
	for _, k := range foundKeys {
		foundKeySet[k] = true
	}

	for _, k := range keys {
		if !foundKeySet[k] {
			responses = append(responses, state.BulkGetResponse{Key: k, Data: nil})
		}
	}

	return responses, nil
}

func (c *Coherence) BulkSet(ctx context.Context, req []state.SetRequest, _ state.BulkStoreOpts) error {
	var (
		err        error
		bytesValue []byte
	)

	err = state.CheckRequestOptions(req)
	if err != nil {
		return err
	}

	buffer := make(map[string][]byte, len(req))

	for _, v := range req {
		bytesValue, err = stateutils.Marshal(v.Value, json.Marshal)
		if err != nil {
			return err
		}
		buffer[v.Key] = bytesValue
	}

	return c.namedCache.PutAll(ctx, buffer)
}

func (c *Coherence) BulkDelete(ctx context.Context, req []state.DeleteRequest, opts state.BulkStoreOpts) error {
	var err error
	err = state.CheckRequestOptions(req)
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(req))
	for _, k := range req {
		keys = append(keys, k.Key)
	}

	return coh.InvokeAllKeysBlind[string, []byte](ctx, c.namedCache, keys, processors.ConditionalRemove(filters.Always()))
}

func (c *Coherence) Ping(ctx context.Context) error {
	ready, err := c.namedCache.IsReady(ctx)
	if err != nil {
		return err
	}

	if ready {
		return nil
	}
	return errors.New("coherence IsReady() returned false")
}

// Features returns the features available in this state store.
func (c *Coherence) Features() []state.Feature {
	return []state.Feature{
		state.FeatureTTL,
	}
}

func (c *Coherence) Set(ctx context.Context, req *state.SetRequest) error {
	ttl := time.Duration(0)
	err := state.CheckRequestOptions(req)
	if err != nil {
		return err
	}
	bytesValue, err := stateutils.Marshal(req.Value, json.Marshal)
	if err != nil {
		return err
	}

	// see if we have a TTL specified
	ttlInSeconds, err := stateutils.ParseTTL64(req.Metadata)
	if err != nil {
		return err
	}
	if ttlInSeconds != nil {
		ttl = time.Duration(*ttlInSeconds) * time.Second
	}

	_, err = c.namedCache.PutWithExpiry(ctx, req.Key, bytesValue, ttl)
	return err
}

func (c *Coherence) Delete(ctx context.Context, req *state.DeleteRequest) error {
	_, err := c.namedCache.Remove(ctx, req.Key)

	return err
}

func (c *Coherence) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	v, err := c.namedCache.Get(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	if v == nil {
		// nothing returned from Coherence
		return &state.GetResponse{}, nil
	}

	// serialize the value to byte
	return &state.GetResponse{
		Data: *v,
	}, nil
}

func (c *Coherence) Close() (err error) {
	if c.session != nil {
		c.session.Close()
	}
	return nil
}

func (c *Coherence) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := coherenceMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}
