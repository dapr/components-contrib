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

// Package mongodb is an implementation of StateStore interface to perform operations on store
package mongodb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	stateutils "github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

const (
	host             = "host"
	username         = "username"
	password         = "password"
	databaseName     = "databaseName"
	collectionName   = "collectionName"
	server           = "server"
	operationTimeout = "operationTimeout"
	params           = "params"
	id               = "_id"
	value            = "value"
	etag             = "_etag"
	ttl              = "_ttl"

	defaultTimeout        = 5 * time.Second
	defaultDatabaseName   = "daprStore"
	defaultCollectionName = "daprCollection"

	// mongodb://<username>:<password@<host>/<database><params>
	connectionURIFormatWithAuthentication = "mongodb://%s:%s@%s/%s%s"

	// mongodb://<host>/<database><params>
	connectionURIFormat = "mongodb://%s/%s%s"

	// mongodb+srv://<server>/<params>
	connectionURIFormatWithSrv = "mongodb+srv://%s/%s"

	// mongodb+srv://<username>:<password>@<server>/<params>
	connectionURIFormatWithSrvAndCredentials = "mongodb+srv://%s:%s@%s/%s%s" //nolint:gosec
)

// MongoDB is a state store implementation for MongoDB.
type MongoDB struct {
	state.BulkStore

	client           *mongo.Client
	collection       *mongo.Collection
	operationTimeout time.Duration
	metadata         mongoDBMetadata

	features     []state.Feature
	logger       logger.Logger
	isReplicaSet bool
}

type mongoDBMetadata struct {
	Host             string
	Username         string
	Password         string
	DatabaseName     string
	CollectionName   string
	Server           string
	Writeconcern     string
	Readconcern      string
	Params           string
	ConnectionString string
	OperationTimeout time.Duration
}

// Item is Mongodb document wrapper.
type Item struct {
	Key   string      `bson:"_id"`
	Value interface{} `bson:"value"`
	Etag  string      `bson:"_etag"`
	TTL   *time.Time  `bson:"_ttl,omitempty"`
}

// NewMongoDB returns a new MongoDB state store.
func NewMongoDB(logger logger.Logger) state.Store {
	s := &MongoDB{
		features: []state.Feature{
			state.FeatureETag,
			state.FeatureTransactional,
			state.FeatureQueryAPI,
			state.FeatureTTL,
		},
		logger: logger,
	}
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

// Init establishes connection to the store based on the metadata.
func (m *MongoDB) Init(ctx context.Context, metadata state.Metadata) (err error) {
	m.metadata, err = getMongoDBMetaData(metadata)
	if err != nil {
		return err
	}

	m.operationTimeout = m.metadata.OperationTimeout

	client, err := m.getMongoDBClient(ctx)
	if err != nil {
		return fmt.Errorf("error in creating mongodb client: %s", err)
	}

	m.client = client

	if err = client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("error in connecting to mongodb, host: %s error: %s", m.metadata.Host, err)
	}

	// get the write concern
	wc, err := getWriteConcernObject(m.metadata.Writeconcern)
	if err != nil {
		return fmt.Errorf("error in getting write concern object: %s", err)
	}

	// get the read concern
	rc, err := getReadConcernObject(m.metadata.Readconcern)
	if err != nil {
		return fmt.Errorf("error in getting read concern object: %s", err)
	}

	opts := options.Collection().SetWriteConcern(wc).SetReadConcern(rc)
	m.collection = m.client.Database(m.metadata.DatabaseName).Collection(m.metadata.CollectionName, opts)

	// Set expireAfterSeconds index on ttl field with a value of 0 to delete
	// values immediately when the TTL value is reached.
	// MongoDB TTL Indexes: https://docs.mongodb.com/manual/core/index-ttl/
	// TTL fields are deleted at most 60 seconds after the TTL value is reached.
	_, err = m.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.M{ttl: 1},
		Options: options.Index().SetExpireAfterSeconds(0),
	})
	if err != nil {
		return fmt.Errorf("error in creating ttl index: %s", err)
	}

	if !m.isReplicaSet {
		m.logger.Info("Connected to MongoDB without a replica set. Transactions are not available, and the component cannot be used as actor state store.")
	}

	return nil
}

// Features returns the features available in this state store.
func (m *MongoDB) Features() []state.Feature {
	return m.features
}

// Set saves state into MongoDB.
func (m *MongoDB) Set(ctx context.Context, req *state.SetRequest) error {
	err := m.setInternal(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (m *MongoDB) Ping(ctx context.Context) error {
	if err := m.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("error connecting to mongoDB at %s: %s", m.metadata.Host, err)
	}

	return nil
}

func (m *MongoDB) setInternal(ctx context.Context, req *state.SetRequest) error {
	var v interface{}
	switch obj := req.Value.(type) {
	case []byte:
		v = string(obj)
	case string:
		v = fmt.Sprintf("%q", obj)
	default:
		v = req.Value
	}

	// create a document based on request key and value
	filter := bson.M{id: req.Key}
	if req.HasETag() {
		filter[etag] = *req.ETag
	} else if req.Options.Concurrency == state.FirstWrite {
		uuid, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		filter[etag] = uuid.String()
	}

	reqTTL, err := stateutils.ParseTTL(req.Metadata)
	if err != nil {
		return fmt.Errorf("failed to parse TTL: %w", err)
	}

	etagV, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	update := make(mongo.Pipeline, 2)
	update[0] = bson.D{{Key: "$set", Value: bson.D{
		{Key: id, Value: req.Key},
		{Key: value, Value: v},
		{Key: etag, Value: etagV.String()},
	}}}

	if reqTTL != nil {
		update[1] = primitive.D{{
			Key: "$addFields", Value: bson.D{
				{
					Key: ttl, Value: bson.D{
						{
							Key: "$add", Value: bson.A{
								// MongoDB stores time in milliseconds so multiply seconds by 1000.
								"$$NOW", *reqTTL * 1000,
							},
						},
					},
				},
			},
		}}
	} else {
		update[1] = primitive.D{
			{Key: "$addFields", Value: bson.D{
				{Key: ttl, Value: nil},
			}},
		}
	}

	_, err = m.collection.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return state.NewETagError(state.ETagMismatch, err)
		}
		return fmt.Errorf("error in updating document: %w", err)
	}

	return nil
}

// Get retrieves state from MongoDB with a key.
func (m *MongoDB) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	filter := bson.D{
		{Key: "$and", Value: bson.A{
			bson.D{{Key: id, Value: bson.M{"$eq": req.Key}}},
			getFilterTTL(),
		}},
	}
	var result Item
	err := m.collection.
		FindOne(ctx, filter).
		Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// Key not found, not an error.
			// To behave the same as other state stores in conf tests.
			err = nil
		}
		return &state.GetResponse{}, err
	}

	data, err := m.decodeData(result.Value)
	if err != nil {
		return &state.GetResponse{}, err
	}

	var metadata map[string]string
	if result.TTL != nil {
		metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: result.TTL.UTC().Format(time.RFC3339),
		}
	}

	return &state.GetResponse{
		Data:     data,
		ETag:     ptr.Of(result.Etag),
		Metadata: metadata,
	}, nil
}

func (m *MongoDB) BulkGet(ctx context.Context, req []state.GetRequest, _ state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	// If nothing is being requested, short-circuit
	if len(req) == 0 {
		return nil, nil
	}

	// Get all the keys
	keys := make(bson.A, len(req))
	for i, r := range req {
		keys[i] = r.Key
	}

	// Perform the query
	filter := bson.D{
		{Key: "$and", Value: bson.A{
			bson.D{
				{Key: id, Value: bson.M{"$in": keys}},
			},
			getFilterTTL(),
		}},
	}
	cur, err := m.collection.Find(ctx, filter)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// No documents found, just return an empty list
			err = nil
		}
		return nil, err
	}
	defer cur.Close(ctx)

	// Read all results
	res := make([]state.BulkGetResponse, 0, len(keys))
	foundKeys := make(map[string]struct{}, len(keys))
	for cur.Next(ctx) {
		var (
			doc  Item
			data []byte
		)
		err = cur.Decode(&doc)
		if err != nil {
			return nil, err
		}

		bgr := state.BulkGetResponse{
			Key: doc.Key,
		}
		if doc.Etag != "" {
			bgr.ETag = ptr.Of(doc.Etag)
		}

		if doc.TTL != nil {
			bgr.Metadata = map[string]string{
				state.GetRespMetaKeyTTLExpireTime: doc.TTL.UTC().Format(time.RFC3339),
			}
		}

		data, err = m.decodeData(doc.Value)
		if err != nil {
			bgr.Error = err.Error()
		} else {
			bgr.Data = data
		}
		res = append(res, bgr)
		foundKeys[bgr.Key] = struct{}{}
	}
	err = cur.Err()
	if err != nil {
		return res, err
	}

	// Populate missing keys with empty values
	// This is to ensure consistency with the other state stores that implement BulkGet as a loop over Get, and with the Get method
	if len(foundKeys) < len(req) {
		var ok bool
		for _, r := range req {
			_, ok = foundKeys[r.Key]
			if !ok {
				res = append(res, state.BulkGetResponse{
					Key: r.Key,
				})
			}
		}
	}

	return res, nil
}

func getFilterTTL() bson.D {
	// Since MongoDB doesn't delete the document immediately when the TTL value
	// is reached, we need to filter out the documents with TTL value less than
	// the current time.
	return bson.D{{Key: "$expr", Value: bson.D{
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "$eq", Value: bson.A{"$_ttl", primitive.Null{}}}},
			bson.D{{Key: "$gte", Value: bson.A{"$_ttl", "$$NOW"}}},
		}},
	}}}
}

func (m *MongoDB) decodeData(resValue any) (data []byte, err error) {
	switch obj := resValue.(type) {
	case string:
		data = []byte(obj)
	case primitive.D, primitive.M:
		if data, err = bson.MarshalExtJSON(obj, true, true); err != nil {
			return nil, err
		}
		vr, errvr := bsonrw.NewExtJSONValueReader(bytes.NewReader(data), true)
		if err != nil {
			return nil, errvr
		}
		decoder, cerr := bson.NewDecoder(vr)
		if cerr != nil {
			return nil, cerr
		}
		var output map[string]interface{}
		if err = decoder.Decode(&output); err != nil {
			return nil, err
		}
		data, err = json.Marshal(output)
		if err != nil {
			return nil, err
		}
	case primitive.A:
		newobj := bson.D{{Key: value, Value: obj}}

		if data, err = bson.MarshalExtJSON(newobj, true, true); err != nil {
			return nil, err
		}
		vr, errvr := bsonrw.NewExtJSONValueReader(bytes.NewReader(data), true)
		if err != nil {
			return nil, errvr
		}
		decoder, cerr := bson.NewDecoder(vr)
		if cerr != nil {
			return nil, cerr
		}
		var input map[string]interface{}
		if err = decoder.Decode(&input); err != nil {
			return nil, err
		}
		value := input[value]
		if data, err = json.Marshal(value); err != nil {
			return nil, err
		}

	default:
		data, err = json.Marshal(obj)
		if err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Delete performs a delete operation.
func (m *MongoDB) Delete(ctx context.Context, req *state.DeleteRequest) error {
	err := m.deleteInternal(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (m *MongoDB) deleteInternal(ctx context.Context, req *state.DeleteRequest) error {
	filter := bson.M{id: req.Key}
	if req.HasETag() {
		filter[etag] = *req.ETag
	}
	result, err := m.collection.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}

	if result.DeletedCount == 0 && req.ETag != nil && *req.ETag != "" {
		return state.NewETagError(state.ETagMismatch, err)
	}

	return nil
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (m *MongoDB) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	if !m.isReplicaSet {
		return errors.New("using transactions with MongoDB requires connecting to a replica set")
	}

	sess, err := m.client.StartSession()
	if err != nil {
		return fmt.Errorf("error starting the transaction: %w", err)
	}
	defer sess.EndSession(ctx)

	txnOpts := options.Transaction().
		SetReadConcern(readconcern.Snapshot()).
		SetWriteConcern(writeconcern.Majority())
	sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		err = m.doTransaction(sessCtx, request.Operations)
		return nil, err
	}, txnOpts)

	return err
}

func (m *MongoDB) doTransaction(sessCtx mongo.SessionContext, operations []state.TransactionalStateOperation) error {
	for _, o := range operations {
		var err error
		switch req := o.(type) {
		case state.SetRequest:
			err = m.setInternal(sessCtx, &req)
		case state.DeleteRequest:
			err = m.deleteInternal(sessCtx, &req)
		}

		if err != nil {
			sessCtx.AbortTransaction(sessCtx)
			return fmt.Errorf("error during transaction, aborting the transaction: %w", err)
		}
	}

	return nil
}

// Query executes a query against store.
func (m *MongoDB) Query(ctx context.Context, req *state.QueryRequest) (*state.QueryResponse, error) {
	q := &Query{}
	qbuilder := query.NewQueryBuilder(q)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{}, err
	}
	data, token, err := q.execute(ctx, m.collection)
	if err != nil {
		return &state.QueryResponse{}, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
}

func (metadata *mongoDBMetadata) getMongoConnectionString() string {
	if metadata.ConnectionString != "" {
		return metadata.ConnectionString
	}

	if len(metadata.Server) != 0 {
		if metadata.Username != "" && metadata.Password != "" {
			return fmt.Sprintf(connectionURIFormatWithSrvAndCredentials, metadata.Username, metadata.Password, metadata.Server, metadata.DatabaseName, metadata.Params)
		}

		return fmt.Sprintf(connectionURIFormatWithSrv, metadata.Server, metadata.Params)
	}

	if metadata.Username != "" && metadata.Password != "" {
		return fmt.Sprintf(connectionURIFormatWithAuthentication, metadata.Username, metadata.Password, metadata.Host, metadata.DatabaseName, metadata.Params)
	}

	return fmt.Sprintf(connectionURIFormat, metadata.Host, metadata.DatabaseName, metadata.Params)
}

func (m *MongoDB) getMongoDBClient(ctx context.Context) (*mongo.Client, error) {
	uri := m.metadata.getMongoConnectionString()

	// Set client options
	clientOptions := options.Client().ApplyURI(uri)
	m.isReplicaSet = clientOptions.ReplicaSet != nil

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(ctx, m.metadata.OperationTimeout)
	defer cancel()

	daprUserAgent := "dapr-" + logger.DaprVersion
	if clientOptions.AppName != nil {
		clientOptions.SetAppName(daprUserAgent + ":" + *clientOptions.AppName)
	} else {
		clientOptions.SetAppName(daprUserAgent)
	}

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getMongoDBMetaData(meta state.Metadata) (mongoDBMetadata, error) {
	m := mongoDBMetadata{
		DatabaseName:     defaultDatabaseName,
		CollectionName:   defaultCollectionName,
		OperationTimeout: defaultTimeout,
	}

	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return m, err
	}

	if m.ConnectionString == "" {
		if len(m.Host) == 0 && len(m.Server) == 0 {
			return m, errors.New("must set 'host' or 'server' fields in metadata")
		}

		if len(m.Host) != 0 && len(m.Server) != 0 {
			return m, errors.New("'host' or 'server' fields are mutually exclusive")
		}

		// Ensure that params, if set, start with ?
		if m.Params != "" && !strings.HasPrefix(m.Params, "?") {
			m.Params = "?" + m.Params
		}
	}

	if val, ok := meta.Properties[operationTimeout]; ok && val != "" {
		m.OperationTimeout, err = time.ParseDuration(val)
		if err != nil {
			return m, errors.New("incorrect operationTimeout field from metadata")
		}
	}

	return m, nil
}

func getWriteConcernObject(cn string) (*writeconcern.WriteConcern, error) {
	var wc *writeconcern.WriteConcern
	if cn != "" {
		if cn == "majority" {
			wc = writeconcern.Majority()
		} else {
			w, err := strconv.Atoi(cn)
			if err != nil {
				return nil, err
			}
			wc = &writeconcern.WriteConcern{
				W: w,
			}
		}
	} else {
		wc = writeconcern.W1()
	}

	wc.Journal = ptr.Of(true)
	wc.WTimeout = defaultTimeout

	return wc, nil
}

func getReadConcernObject(cn string) (*readconcern.ReadConcern, error) {
	switch cn {
	case "local":
		return readconcern.Local(), nil
	case "majority":
		return readconcern.Majority(), nil
	case "available":
		return readconcern.Available(), nil
	case "linearizable":
		return readconcern.Linearizable(), nil
	case "snapshot":
		return readconcern.Snapshot(), nil
	case "":
		return readconcern.Local(), nil
	}

	return nil, fmt.Errorf("readConcern %s not found", cn)
}

func (m *MongoDB) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := mongoDBMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

// Close connection to the database.
func (m *MongoDB) Close() error {
	if m.client == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	return m.client.Disconnect(ctx)
}
