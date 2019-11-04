// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package couchbase

import (
	"errors"
	"fmt"

	"github.com/dapr/components-contrib/state"
	"gopkg.in/couchbase/gocb.v1"

	jsoniter "github.com/json-iterator/go"
)

const (
	couchbaseURL = "couchbaseURL"
	username     = "username"
	password     = "password"
	bucket       = "bucket"
)

// Couchbase is a couchbase state store
type Couchbase struct {
	cbCluster *gocb.Cluster
	bucket    string //TODO: having bucket sent as part of request (get,set etc.) metadata would be more flexible
	json      jsoniter.API
}

// NewCouchbaseStateStore returns a new couchbase state store
func NewCouchbaseStateStore() *Couchbase {
	return &Couchbase{
		json: jsoniter.ConfigFastest,
	}
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

	if metadata.Properties[bucket] == "" {
		return errors.New("couchbase error: couchbase bucket is missing")
	}

	return nil
}

// Init does metadata and connection parsing
func (cbs *Couchbase) Init(metadata state.Metadata) error {
	err := validateMetadata(metadata)
	if err != nil {
		return err
	}
	cbs.bucket = metadata.Properties[bucket]
	c, err := gocb.Connect(metadata.Properties[couchbaseURL])
	if err != nil {
		return fmt.Errorf("couchbase error: unable to connect to couchbase at %s - %v ", metadata.Properties[couchbaseURL], err)
	}
	cbs.cbCluster = c
	//does not actually trigger the authentication
	cbs.cbCluster.Authenticate(gocb.PasswordAuthenticator{
		Username: metadata.Properties[username],
		Password: metadata.Properties[password],
	})
	return nil
}

//Set stores value for a key to couchbase
func (cbs *Couchbase) Set(req *state.SetRequest) error {
	err := state.CheckSetRequestOptions(req)
	if err != nil {
		return err
	}
	var value string
	b, ok := req.Value.([]byte)
	if ok {
		value = string(b)
	} else {
		value, err = cbs.json.MarshalToString(req.Value)
	}

	if err != nil {
		return fmt.Errorf("couchbase error: failed to convert value %v", err)
	}
	//Under RBAC, bucket-passwords are no longer used
	//see https://docs.couchbase.com/go-sdk/1.6/sdk-authentication-overview.html#authenticating-with-legacy-sdk-versions
	bucket, err := cbs.cbCluster.OpenBucket(cbs.bucket, "")
	if err != nil {
		return fmt.Errorf("couchbase error: failed to open bucket %s - %v", cbs.bucket, err)
	}
	defer bucket.Close()

	_, err = bucket.Upsert(req.Key, value, 0)

	if err != nil {
		return fmt.Errorf("couchbase error: failed to set value for key %s - %v", req.Key, err)
	}

	return nil
}

// BulkSet performs a bulks save operation
func (cbs *Couchbase) BulkSet(req []state.SetRequest) error {
	for _, s := range req {
		err := cbs.Set(&s)
		if err != nil {
			return err
		}
	}

	return nil
}

// Get retrieves state from couchbase with a key
func (cbs *Couchbase) Get(req *state.GetRequest) (*state.GetResponse, error) {
	bucket, err := cbs.cbCluster.OpenBucket(cbs.bucket, "")
	if err != nil {
		return nil, fmt.Errorf("couchbase error: failed to open bucket %s - %v", cbs.bucket, err)
	}
	defer bucket.Close()
	var data interface{}
	cas, err := bucket.Get(req.Key, &data)
	if err != nil {
		return nil, fmt.Errorf("couchbase error: failed to get value for key %s - %v", req.Key, err)
	}
	value, err := cbs.json.Marshal(&data)

	if err != nil {
		return nil, fmt.Errorf("couchbase error: failed to convert value to byte[] - %v", err)
	}

	return &state.GetResponse{
		Data: value,
		ETag: fmt.Sprintf("%d", cas),
	}, nil
}

// Delete performs a delete operation
func (cbs *Couchbase) Delete(req *state.DeleteRequest) error {
	err := state.CheckDeleteRequestOptions(req)
	if err != nil {
		return err
	}
	bucket, err := cbs.cbCluster.OpenBucket(cbs.bucket, "")
	if err != nil {
		return fmt.Errorf("couchbase error: failed to open bucket %s - %v", cbs.bucket, err)
	}
	defer bucket.Close()
	//Remove accepts Cas which can be sent as ETag, but its not compulsory for DeleteRequest
	//setting Cas to 0
	_, err = bucket.Remove(req.Key, 0)
	if err != nil {
		return fmt.Errorf("couchbase error: failed to delete key %s - %v", req.Key, err)
	}

	return nil
}

// BulkDelete performs a bulk delete operation
func (cbs *Couchbase) BulkDelete(req []state.DeleteRequest) error {
	for _, re := range req {
		err := cbs.Delete(&re)
		if err != nil {
			return err
		}
	}

	return nil
}
