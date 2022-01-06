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

package couchbase

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/agrea/ptr"
	jsoniter "github.com/json-iterator/go"
	"gopkg.in/couchbase/gocb.v1"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

const (
	couchbaseURL = "couchbaseURL"
	username     = "username"
	password     = "password"
	bucketName   = "bucketName"

	// see https://docs.couchbase.com/go-sdk/1.6/durability.html#configuring-durability
	numReplicasDurableReplication = "numReplicasDurableReplication"
	numReplicasDurablePersistence = "numReplicasDurablePersistence"
)

// Couchbase is a couchbase state store.
type Couchbase struct {
	state.DefaultBulkStore
	bucket                        *gocb.Bucket
	bucketName                    string // TODO: having bucket name sent as part of request (get,set etc.) metadata would be more flexible
	numReplicasDurableReplication uint
	numReplicasDurablePersistence uint
	json                          jsoniter.API

	features []state.Feature
	logger   logger.Logger
}

// NewCouchbaseStateStore returns a new couchbase state store.
func NewCouchbaseStateStore(logger logger.Logger) *Couchbase {
	s := &Couchbase{
		json:     jsoniter.ConfigFastest,
		features: []state.Feature{state.FeatureETag},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func validateMetadata(metadata state.Metadata) error {
	if metadata.Properties[couchbaseURL] == "" {
		return errors.New("couchbase error: couchbase URL is missing")
	}

	if metadata.Properties[username] == "" {
		return errors.New("couchbase error: couchbase username is missing")
	}

	if metadata.Properties[password] == "" {
		return errors.New("couchbase error: couchbase password is missing")
	}

	if metadata.Properties[bucketName] == "" {
		return errors.New("couchbase error: couchbase bucket name is missing")
	}

	v := metadata.Properties[numReplicasDurableReplication]
	if v != "" {
		_, err := strconv.ParseUint(v, 10, 0)
		if err != nil {
			return fmt.Errorf("couchbase error: %v", err)
		}
	}

	v = metadata.Properties[numReplicasDurablePersistence]
	if v != "" {
		_, err := strconv.ParseUint(v, 10, 0)
		if err != nil {
			return fmt.Errorf("couchbase error: %v", err)
		}
	}

	return nil
}

// Init does metadata and connection parsing.
func (cbs *Couchbase) Init(metadata state.Metadata) error {
	err := validateMetadata(metadata)
	if err != nil {
		return err
	}
	cbs.bucketName = metadata.Properties[bucketName]
	c, err := gocb.Connect(metadata.Properties[couchbaseURL])
	if err != nil {
		return fmt.Errorf("couchbase error: unable to connect to couchbase at %s - %v ", metadata.Properties[couchbaseURL], err)
	}
	// does not actually trigger the authentication
	c.Authenticate(gocb.PasswordAuthenticator{
		Username: metadata.Properties[username],
		Password: metadata.Properties[password],
	})

	// with RBAC, bucket-passwords are no longer used - https://docs.couchbase.com/go-sdk/1.6/sdk-authentication-overview.html#authenticating-with-legacy-sdk-versions
	bucket, err := c.OpenBucket(cbs.bucketName, "")
	if err != nil {
		return fmt.Errorf("couchbase error: failed to open bucket %s - %v", cbs.bucketName, err)
	}
	cbs.bucket = bucket

	r := metadata.Properties[numReplicasDurableReplication]
	if r != "" {
		_r, _ := strconv.ParseUint(r, 10, 0)
		cbs.numReplicasDurableReplication = uint(_r)
	}

	p := metadata.Properties[numReplicasDurablePersistence]
	if p != "" {
		_p, _ := strconv.ParseUint(p, 10, 0)
		cbs.numReplicasDurablePersistence = uint(_p)
	}

	return nil
}

// Features returns the features available in this state store.
func (cbs *Couchbase) Features() []state.Feature {
	return cbs.features
}

// Set stores value for a key to couchbase. It honors ETag (for concurrency) and consistency settings.
func (cbs *Couchbase) Set(req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}
	value, err := utils.Marshal(req.Value, cbs.json.Marshal)
	if err != nil {
		return fmt.Errorf("couchbase error: failed to convert value %v", err)
	}

	// nolint:nestif
	// key already exists (use Replace)
	if req.ETag != nil {
		// compare-and-swap (CAS) for managing concurrent modifications - https://docs.couchbase.com/go-sdk/current/concurrent-mutations-cluster.html
		cas, cerr := eTagToCas(*req.ETag)
		if cerr != nil {
			return err
		}
		if req.Options.Consistency == state.Strong {
			_, err = cbs.bucket.ReplaceDura(req.Key, value, cas, 0, cbs.numReplicasDurableReplication, cbs.numReplicasDurablePersistence)
		} else {
			_, err = cbs.bucket.Replace(req.Key, value, cas, 0)
		}
	} else {
		// key does not exist: replace or insert (with Upsert)
		if req.Options.Consistency == state.Strong {
			_, err = cbs.bucket.UpsertDura(req.Key, value, 0, cbs.numReplicasDurableReplication, cbs.numReplicasDurablePersistence)
		} else {
			_, err = cbs.bucket.Upsert(req.Key, value, 0)
		}
	}

	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("couchbase error: failed to set value for key %s - %v", req.Key, err)
	}

	return nil
}

// Get retrieves state from couchbase with a key.
func (cbs *Couchbase) Get(req *state.GetRequest) (*state.GetResponse, error) {
	var data interface{}
	cas, err := cbs.bucket.Get(req.Key, &data)
	if err != nil {
		if gocb.IsKeyNotFoundError(err) {
			return &state.GetResponse{}, nil
		}

		return nil, fmt.Errorf("couchbase error: failed to get value for key %s - %v", req.Key, err)
	}

	return &state.GetResponse{
		Data: data.([]byte),
		ETag: ptr.String(strconv.FormatUint(uint64(cas), 10)),
	}, nil
}

// Delete performs a delete operation.
func (cbs *Couchbase) Delete(req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	var cas gocb.Cas = 0

	if req.ETag != nil {
		cas, err = eTagToCas(*req.ETag)
		if err != nil {
			return err
		}
	}
	if req.Options.Consistency == state.Strong {
		_, err = cbs.bucket.RemoveDura(req.Key, cas, cbs.numReplicasDurableReplication, cbs.numReplicasDurablePersistence)
	} else {
		_, err = cbs.bucket.Remove(req.Key, cas)
	}
	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("couchbase error: failed to delete key %s - %v", req.Key, err)
	}

	return nil
}

func (cbs *Couchbase) Ping() error {
	return nil
}

// converts string etag sent by the application into a gocb.Cas object, which can then be used for optimistic locking for set and delete operations.
func eTagToCas(eTag string) (gocb.Cas, error) {
	var cas gocb.Cas = 0
	// CAS is a 64-bit integer - https://docs.couchbase.com/go-sdk/current/concurrent-mutations-cluster.html#cas-value-format
	temp, err := strconv.ParseUint(eTag, 10, 64)
	if err != nil {
		return cas, state.NewETagError(state.ETagInvalid, err)
	}
	cas = gocb.Cas(temp)

	return cas, nil
}
