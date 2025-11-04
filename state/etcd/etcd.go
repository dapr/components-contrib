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

package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
	kitstrings "github.com/dapr/kit/strings"
)

// Etcd is a state store implementation for Etcd.
type Etcd struct {
	state.BulkStore

	client        *clientv3.Client
	keyPrefixPath string
	features      []state.Feature
	logger        logger.Logger
	schema        schemaMarshaller
	maxTxnOps     int
}

type etcdConfig struct {
	Endpoints     string `json:"endpoints"`
	KeyPrefixPath string `json:"keyPrefixPath"`
	// TLS
	TLSEnable string `json:"tlsEnable"`
	CA        string `json:"ca"`
	Cert      string `json:"cert"`
	Key       string `json:"key"`
	// Transaction server options
	MaxTxnOps int `mapstructure:"maxTxnOps"`
}

// NewEtcdStateStoreV1 returns a new etcd state store for schema V1.
func NewEtcdStateStoreV1(logger logger.Logger) state.Store {
	return newETCD(logger, schemaV1{})
}

// NewEtcdStateStoreV2 returns a new etcd state store for schema V2.
func NewEtcdStateStoreV2(logger logger.Logger) state.Store {
	return newETCD(logger, schemaV2{})
}

func newETCD(logger logger.Logger, schema schemaMarshaller) state.Store {
	s := &Etcd{
		schema: schema,
		logger: logger,
		features: []state.Feature{
			state.FeatureETag,
			state.FeatureTransactional,
			state.FeatureTTL,
			state.FeatureKeysLike,
		},
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// Init does metadata and config parsing and initializes the
// Etcd client.
func (e *Etcd) Init(_ context.Context, metadata state.Metadata) error {
	etcdConfig, err := metadataToConfig(metadata.Properties)
	if err != nil {
		return fmt.Errorf("couldn't convert metadata properties: %w", err)
	}

	e.client, err = e.ParseClientFromConfig(etcdConfig)
	if err != nil {
		return fmt.Errorf("initializing etcd client: %w", err)
	}

	e.keyPrefixPath = etcdConfig.KeyPrefixPath
	e.maxTxnOps = etcdConfig.MaxTxnOps

	return nil
}

func (e *Etcd) ParseClientFromConfig(etcdConfig *etcdConfig) (*clientv3.Client, error) {
	endpoints := strings.Split(etcdConfig.Endpoints, ",")
	if len(endpoints) == 0 || endpoints[0] == "" {
		return nil, errors.New("endpoints required")
	}

	var tlsConfig *tls.Config
	if kitstrings.IsTruthy(etcdConfig.TLSEnable) {
		if etcdConfig.Cert != "" && etcdConfig.Key != "" && etcdConfig.CA != "" {
			var err error
			tlsConfig, err = NewTLSConfig(etcdConfig.Cert, etcdConfig.Key, etcdConfig.CA)
			if err != nil {
				return nil, fmt.Errorf("tls authentication error: %w", err)
			}
		} else {
			return nil, errors.New("tls authentication information is incomplete")
		}
	}

	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second, // TODO: make this configurable
		TLS:         tlsConfig,
	}
	client, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Features returns the features available in this state store.
func (e *Etcd) Features() []state.Feature {
	return e.features
}

func metadataToConfig(connInfo map[string]string) (*etcdConfig, error) {
	m := &etcdConfig{
		// This is the default value for maximum ops per transaction, configurtable via etcd server flag --max-txn-ops.
		MaxTxnOps: 128,
	}
	err := kitmd.DecodeMetadata(connInfo, m)
	return m, err
}

// Get retrieves a Etcd KV item.
func (e *Etcd) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	keyWithPath := e.keyPrefixPath + "/" + req.Key

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := e.client.Get(ctx, keyWithPath)
	if err != nil {
		return nil, fmt.Errorf("couldn't get key %s: %w", keyWithPath, err)
	}

	if resp == nil || len(resp.Kvs) == 0 {
		return &state.GetResponse{}, nil
	}

	data, metadata, err := e.schema.decode(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data:     data,
		ETag:     ptr.Of(strconv.Itoa(int(resp.Kvs[0].ModRevision))),
		Metadata: metadata,
	}, nil
}

// Set saves a Etcd KV item.
func (e *Etcd) Set(ctx context.Context, req *state.SetRequest) error {
	ttlInSeconds, err := e.doSetValidateParameters(req)
	if err != nil {
		return err
	}

	keyWithPath := e.keyPrefixPath + "/" + req.Key
	err = e.doValidateEtag(keyWithPath, req.ETag, req.Options.Concurrency)
	if err != nil {
		return err
	}

	return e.doSet(ctx, keyWithPath, req.Value, req.ETag, ttlInSeconds)
}

func (e *Etcd) doSet(ctx context.Context, key string, val any, etag *string, ttlInSeconds *int64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	reqVal, err := e.schema.encode(val, ttlInSeconds)
	if err != nil {
		return err
	}

	var leaseID clientv3.LeaseID
	if ttlInSeconds != nil {
		var resp *clientv3.LeaseGrantResponse
		resp, err = e.client.Grant(ctx, *ttlInSeconds)
		if err != nil {
			return fmt.Errorf("couldn't grant lease %s: %w", key, err)
		}
		leaseID = resp.ID
	}

	if etag != nil {
		etag, _ := strconv.ParseInt(*etag, 10, 64)
		_, err = e.client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", etag)).
			Then(clientv3.OpPut(key, reqVal, clientv3.WithLease(leaseID))).
			Commit()
	} else {
		_, err = e.client.Put(ctx, key, reqVal, clientv3.WithLease(leaseID))
	}
	if err != nil {
		return fmt.Errorf("couldn't set key %s: %w", key, err)
	}

	return nil
}

func (e *Etcd) doSetValidateParameters(req *state.SetRequest) (*int64, error) {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return nil, err
	}

	ttlInSeconds, err := stateutils.ParseTTL64(req.Metadata)
	if err != nil {
		return nil, err
	}

	return ttlInSeconds, nil
}

// Delete performes a Etcd KV delete operation.
func (e *Etcd) Delete(ctx context.Context, req *state.DeleteRequest) error {
	if err := state.CheckRequestOptions(req.Options); err != nil {
		return err
	}

	keyWithPath := e.keyPrefixPath + "/" + req.Key

	if err := e.doValidateEtag(keyWithPath, req.ETag, req.Options.Concurrency); err != nil {
		return err
	}

	return e.doDelete(ctx, keyWithPath, req.ETag)
}

func (e *Etcd) doDelete(ctx context.Context, key string, etag *string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var err error
	if etag != nil {
		etag, _ := strconv.ParseInt(*etag, 10, 64)
		_, err = e.client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", etag)).
			Then(clientv3.OpDelete(key)).
			Commit()
	} else {
		_, err = e.client.Delete(ctx, key)
	}
	if err != nil {
		return fmt.Errorf("couldn't delete key %s: %w", key, err)
	}
	return nil
}

func (e *Etcd) doValidateEtag(key string, etag *string, concurrency string) error {
	hasEtag := etag != nil && *etag != ""
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if concurrency == state.FirstWrite && !hasEtag {
		item, err := e.client.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("couldn't get key %s: %w", key, err)
		} else if item != nil && len(item.Kvs) != 0 {
			return state.NewETagError(state.ETagMismatch, errors.New("item already exists and no etag was passed"))
		} else {
			return nil
		}
	} else if hasEtag {
		item, err := e.client.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("couldn't get key %s: %w", key, err)
		}
		if item == nil || len(item.Kvs) == 0 {
			return state.NewETagError(state.ETagMismatch, fmt.Errorf("state not exist or expired for key=%s", key))
		}
		currentEtag := strconv.Itoa(int(item.Kvs[0].ModRevision))
		if currentEtag != *etag {
			return state.NewETagError(state.ETagMismatch, fmt.Errorf(
				"state etag not match for key=%s: current=%s, expect=%s", key, currentEtag, *etag))
		}
	}
	return nil
}

func (e *Etcd) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := etcdConfig{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func (e *Etcd) Close() error {
	if e.client == nil {
		return nil
	}

	return e.client.Close()
}

func (e *Etcd) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := e.client.Get(ctx, "health"); err != nil {
		return fmt.Errorf("etcd store: error connecting to etcd at %s: %w", e.client.Endpoints(), err)
	}

	return nil
}

// MultiMaxSize returns the maximum number of operations allowed in a transaction.
// For Etcd the default is 128, but this can be configured via the server flag --max-txn-ops.
// As such we are using the component metadata value maxTxnOps.
func (e *Etcd) MultiMaxSize() int {
	return e.maxTxnOps
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (e *Etcd) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	if len(request.Operations) == 0 {
		return nil
	}

	ops := make([]clientv3.Op, 0, len(request.Operations))

	for _, o := range request.Operations {
		switch req := o.(type) {
		case state.SetRequest:
			ttlInSeconds, err := e.doSetValidateParameters(&req)
			if err != nil {
				return err
			}

			keyWithPath := e.keyPrefixPath + "/" + req.Key
			err = e.doValidateEtag(keyWithPath, req.ETag, req.Options.Concurrency)
			if err != nil {
				return err
			}

			reqVal, err := e.schema.encode(req.Value, ttlInSeconds)
			if err != nil {
				return err
			}
			var cmp clientv3.Cmp
			if req.HasETag() {
				etag, _ := strconv.ParseInt(*req.ETag, 10, 64)
				cmp = clientv3.Compare(clientv3.ModRevision(keyWithPath), "=", etag)
			}
			if ttlInSeconds != nil {
				resp, err := e.client.Grant(ctx, *ttlInSeconds)
				if err != nil {
					return fmt.Errorf("couldn't grant lease %s: %w", keyWithPath, err)
				}
				put := clientv3.OpPut(keyWithPath, reqVal, clientv3.WithLease(resp.ID))
				if req.HasETag() {
					ops = append(ops, clientv3.OpTxn([]clientv3.Cmp{cmp}, []clientv3.Op{put}, nil))
				} else {
					ops = append(ops, clientv3.OpTxn(nil, []clientv3.Op{put}, nil))
				}
			} else {
				put := clientv3.OpPut(keyWithPath, reqVal)
				if req.HasETag() {
					ops = append(ops, clientv3.OpTxn([]clientv3.Cmp{cmp}, []clientv3.Op{put}, nil))
				} else {
					ops = append(ops, clientv3.OpTxn(nil, []clientv3.Op{put}, nil))
				}
			}
		case state.DeleteRequest:
			if err := state.CheckRequestOptions(req.Options); err != nil {
				return err
			}

			keyWithPath := e.keyPrefixPath + "/" + req.Key
			err := e.doValidateEtag(keyWithPath, req.ETag, req.Options.Concurrency)
			if err != nil {
				return err
			}

			del := clientv3.OpDelete(keyWithPath)
			if req.HasETag() {
				etag, _ := strconv.ParseInt(*req.ETag, 10, 64)
				cmp := clientv3.Compare(clientv3.ModRevision(keyWithPath), "=", etag)
				ops = append(ops, clientv3.OpTxn([]clientv3.Cmp{cmp}, []clientv3.Op{del}, nil))
			} else {
				ops = append(ops, clientv3.OpTxn(nil, []clientv3.Op{del}, nil))
			}
		}
	}

	_, err := e.client.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return err
	}

	return nil
}

func NewTLSConfig(clientCert, clientKey, caCert string) (*tls.Config, error) {
	valid := false

	config := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if clientCert != "" && clientKey != "" {
		key := []byte(clientKey)
		cert, err := tls.X509KeyPair([]byte(clientCert), key)
		if err != nil {
			return nil, fmt.Errorf("error parse X509KeyPair: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
		valid = true
	}

	if caCert != "" {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(caCert))
		config.RootCAs = caCertPool
		valid = true
	}

	if !valid {
		config = nil
	}

	return config, nil
}

func (e *Etcd) KeysLike(ctx context.Context, req *state.KeysLikeRequest) (*state.KeysLikeResponse, error) {
	if len(req.Pattern) == 0 {
		return nil, state.ErrKeysLikeEmptyPattern
	}

	// Build the etcd key prefix we need to scan, using the literal prefix
	// (up to the first unescaped % or _) to keep scans narrow.
	userPrefix := likeLiteralPrefix(req.Pattern)
	etcdPrefix := strings.TrimSuffix(e.keyPrefixPath, "/") + "/" + userPrefix

	// Fetch keys under that etcd prefix
	// (we read values too to safely get revisions; KeysOnly omits CreateRevision on some clients).
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := e.client.Get(ctx, etcdPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	// Prepare paging with CreateRevision (monotonic “row id”)
	var afterRev int64
	if req.ContinueToken != nil && *req.ContinueToken != "" {
		// ignore parse errors to be conservative: invalid token => no results
		if v, perr := strconv.ParseInt(*req.ContinueToken, 10, 64); perr == nil {
			afterRev = v
		} else {
			return nil, fmt.Errorf("invalid continue token: %w", perr)
		}
	}

	type rec struct {
		key string
		rev int64 // CreateRevision
	}
	recs := make([]rec, 0, len(resp.Kvs))

	// Collect user keys that match the LIKE pattern and are not expired
	base := strings.TrimSuffix(e.keyPrefixPath, "/") + "/"
	for _, kv := range resp.Kvs {
		// Extract the user key (strip the configured prefix path)
		fullKey := string(kv.Key)
		if !strings.HasPrefix(fullKey, base) {
			continue
		}
		userKey := fullKey[len(base):]

		// SQL LIKE match with backslash escapes
		if !likeMatch(userKey, req.Pattern) {
			continue
		}

		// Filter by CreateRevision for paging
		if afterRev > 0 && kv.CreateRevision <= afterRev {
			continue
		}

		recs = append(recs, rec{key: userKey, rev: kv.CreateRevision})
	}

	// Sort by CreateRevision ascending to mimic a stable “row_id” order
	sort.Slice(recs, func(i, j int) bool { return recs[i].rev < recs[j].rev })

	respOut := &state.KeysLikeResponse{Keys: make([]string, 0, len(recs))}

	// Apply page size (fetch one extra to decide if there is a next page)
	if req.PageSize != nil && *req.PageSize > 0 {
		ps := int(*req.PageSize)
		if len(recs) > ps {
			// Continue token is the CreateRevision of the LAST returned item.
			respOut.ContinueToken = ptr.Of(strconv.FormatInt(recs[ps-1].rev, 10))
			recs = recs[:ps]
		}
	}

	for _, r := range recs {
		respOut.Keys = append(respOut.Keys, r.key)
	}

	return respOut, nil
}

// likeLiteralPrefix returns the literal prefix before the first unescaped % or _.
func likeLiteralPrefix(p string) string {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch c {
		case '\\':
			if i+1 < len(p) {
				b.WriteByte(p[i+1])
				i++
			} else {
				// Trailing backslash: treat it literally.
				b.WriteByte('\\')
			}
		case '%', '_':
			return b.String()
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// likeMatch implements SQL LIKE for ASCII with % (any) and _ (single char).
// Backslash escapes %, _, and \ (as used in the conformance tests).
func likeMatch(s, p string) bool {
	i, j := 0, 0
	star := -1 // position of last % in pattern
	match := 0 // index in s where we started to match after last %
	for i < len(s) {
		if j < len(p) {
			switch p[j] {
			case '\\':
				// Escape next char, must match literally
				if j+1 >= len(p) {
					// dangling escape => treat as literal '\'
					if s[i] != '\\' {
						goto backtrack
					}
					i++
					j++
					continue
				}
				j++
				if s[i] == p[j] {
					i++
					j++
					continue
				}
				goto backtrack
			case '_':
				// Match any single char
				i++
				j++
				continue
			case '%':
				// Remember position of % and try to match zero chars first
				star = j
				match = i
				j++
				continue
			default:
				if s[i] == p[j] {
					i++
					j++
					continue
				}
			}
		}
	backtrack:
		if star != -1 {
			// Backtrack: extend % to cover one more char
			j = star + 1
			match++
			i = match
			continue
		}
		return false
	}
	// Consume trailing % (and escaped sequences like "\%" are not %)
	for j < len(p) {
		if p[j] == '%' {
			j++
			continue
		}
		if p[j] == '\\' {
			// Escaped literal remains unmatched since s ended
			// If there's a char after '\', it cannot match empty
			return false
		}
		// Any other char (including '_') cannot match empty
		return false
	}
	return true
}
