// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mongodb

// mongodb package is an implementation of StateStore interface to perform operations on store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/agrea/ptr"
	"github.com/google/uuid"
	json "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const (
	host             = "host"
	username         = "username"
	password         = "password"
	databaseName     = "databaseName"
	collectionName   = "collectionName"
	server           = "server"
	writeConcern     = "writeConcern"
	readConcern      = "readConcern"
	operationTimeout = "operationTimeout"
	params           = "params"
	id               = "_id"
	value            = "value"
	etag             = "_etag"

	defaultTimeout        = 5 * time.Second
	defaultDatabaseName   = "daprStore"
	defaultCollectionName = "daprCollection"

	// mongodb://<username>:<password@<host>/<database><params>
	connectionURIFormatWithAuthentication = "mongodb://%s:%s@%s/%s%s"

	// mongodb://<host>/<database><params>
	connectionURIFormat = "mongodb://%s/%s%s"

	// mongodb+srv://<server>/<params>
	connectionURIFormatWithSrv = "mongodb+srv://%s/%s"
)

// MongoDB is a state store implementation for MongoDB
type MongoDB struct {
	state.DefaultBulkStore
	client           *mongo.Client
	collection       *mongo.Collection
	operationTimeout time.Duration
	metadata         mongoDBMetadata

	features []state.Feature
	logger   logger.Logger
}

type mongoDBMetadata struct {
	host             string
	username         string
	password         string
	databaseName     string
	collectionName   string
	server           string
	writeconcern     string
	readconcern      string
	params           string
	operationTimeout time.Duration
}

// Item is Mongodb document wrapper
type Item struct {
	Key   string `bson:"_id"`
	Value string `bson:"value"`
	Etag  string `bson:"_etag"`
}

// NewMongoDB returns a new MongoDB state store
func NewMongoDB(logger logger.Logger) *MongoDB {
	s := &MongoDB{
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init establishes connection to the store based on the metadata
func (m *MongoDB) Init(metadata state.Metadata) error {
	meta, err := getMongoDBMetaData(metadata)
	if err != nil {
		return err
	}

	m.operationTimeout = meta.operationTimeout

	client, err := getMongoDBClient(meta)
	if err != nil {
		return fmt.Errorf("error in creating mongodb client: %s", err)
	}

	if err = client.Ping(context.Background(), nil); err != nil {
		return fmt.Errorf("error in connecting to mongodb, host: %s error: %s", meta.host, err)
	}

	m.client = client

	// get the write concern
	wc, err := getWriteConcernObject(meta.writeconcern)
	if err != nil {
		return fmt.Errorf("error in getting write concern object: %s", err)
	}

	// get the read concern
	rc, err := getReadConcernObject(meta.readconcern)
	if err != nil {
		return fmt.Errorf("error in getting read concern object: %s", err)
	}

	m.metadata = *meta
	opts := options.Collection().SetWriteConcern(wc).SetReadConcern(rc)
	collection := m.client.Database(meta.databaseName).Collection(meta.collectionName, opts)

	m.collection = collection

	return nil
}

// Features returns the features available in this state store
func (m *MongoDB) Features() []state.Feature {
	return m.features
}

// Set saves state into MongoDB
func (m *MongoDB) Set(req *state.SetRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), m.operationTimeout)
	defer cancel()

	err := m.setInternal(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (m *MongoDB) Ping() error {
	if err := m.client.Ping(context.Background(), nil); err != nil {
		return fmt.Errorf("mongoDB store: error connecting to mongoDB at %s: %s", m.metadata.host, err)
	}

	return nil
}

func (m *MongoDB) setInternal(ctx context.Context, req *state.SetRequest) error {
	var vStr string
	b, ok := req.Value.([]byte)
	if ok {
		vStr = string(b)
	} else {
		vStr, _ = json.MarshalToString(req.Value)
	}

	// create a document based on request key and value
	filter := bson.M{id: req.Key}
	if req.ETag != nil {
		filter[etag] = *req.ETag
	}

	update := bson.M{"$set": bson.M{id: req.Key, value: vStr, etag: uuid.NewString()}}
	_, err := m.collection.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return err
	}

	return nil
}

// Get retrieves state from MongoDB with a key
func (m *MongoDB) Get(req *state.GetRequest) (*state.GetResponse, error) {
	var result Item

	ctx, cancel := context.WithTimeout(context.Background(), m.operationTimeout)
	defer cancel()

	filter := bson.M{id: req.Key}
	err := m.collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err.Error() == "mongo: no documents in result" {
			// Key not found, not an error.
			// To behave the same as other state stores in conf tests.
			return &state.GetResponse{}, nil
		}

		return &state.GetResponse{}, err
	}

	value := []byte(result.Value)

	return &state.GetResponse{
		Data: value,
		ETag: ptr.String(result.Etag),
	}, nil
}

// Delete performs a delete operation
func (m *MongoDB) Delete(req *state.DeleteRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), m.operationTimeout)
	defer cancel()

	err := m.deleteInternal(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (m *MongoDB) deleteInternal(ctx context.Context, req *state.DeleteRequest) error {
	filter := bson.M{id: req.Key}
	if req.ETag != nil {
		filter[etag] = *req.ETag
	}
	result, err := m.collection.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}

	if result.DeletedCount == 0 && req.ETag != nil {
		return errors.New("key or etag not found")
	}

	return nil
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail
func (m *MongoDB) Multi(request *state.TransactionalStateRequest) error {
	sess, err := m.client.StartSession()
	txnOpts := options.Transaction().SetReadConcern(readconcern.Snapshot()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))

	defer sess.EndSession(context.Background())

	if err != nil {
		return fmt.Errorf("error in starting the transaction: %s", err)
	}

	sess.WithTransaction(context.Background(), func(sessCtx mongo.SessionContext) (interface{}, error) {
		err = m.doTransaction(sessCtx, request.Operations)

		return nil, err
	}, txnOpts)

	return err
}

func (m *MongoDB) doTransaction(sessCtx mongo.SessionContext, operations []state.TransactionalStateOperation) error {
	for _, o := range operations {
		var err error
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)
			err = m.setInternal(sessCtx, &req)
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)
			err = m.deleteInternal(sessCtx, &req)
		}

		if err != nil {
			sessCtx.AbortTransaction(sessCtx)

			return fmt.Errorf("error during transaction, aborting the transaction: %s", err)
		}
	}

	return nil
}

func getMongoURI(metadata *mongoDBMetadata) string {
	if len(metadata.server) != 0 {
		return fmt.Sprintf(connectionURIFormatWithSrv, metadata.server, metadata.params)
	}

	if metadata.username != "" && metadata.password != "" {
		return fmt.Sprintf(connectionURIFormatWithAuthentication, metadata.username, metadata.password, metadata.host, metadata.databaseName, metadata.params)
	}

	return fmt.Sprintf(connectionURIFormat, metadata.host, metadata.databaseName, metadata.params)
}

func getMongoDBClient(metadata *mongoDBMetadata) (*mongo.Client, error) {
	uri := getMongoURI(metadata)

	// Set client options
	clientOptions := options.Client().ApplyURI(uri)

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), metadata.operationTimeout)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getMongoDBMetaData(metadata state.Metadata) (*mongoDBMetadata, error) {
	meta := mongoDBMetadata{
		databaseName:     defaultDatabaseName,
		collectionName:   defaultCollectionName,
		operationTimeout: defaultTimeout,
	}

	if val, ok := metadata.Properties[host]; ok && val != "" {
		meta.host = val
	}

	if val, ok := metadata.Properties[server]; ok && val != "" {
		meta.server = val
	}

	if len(meta.host) == 0 && len(meta.server) == 0 {
		return nil, errors.New("must set 'host' or 'server' fields in metadata")
	}

	if len(meta.host) != 0 && len(meta.server) != 0 {
		return nil, errors.New("'host' or 'server' fields are mutually exclusive")
	}

	if val, ok := metadata.Properties[username]; ok && val != "" {
		meta.username = val
	}

	if val, ok := metadata.Properties[password]; ok && val != "" {
		meta.password = val
	}

	if val, ok := metadata.Properties[databaseName]; ok && val != "" {
		meta.databaseName = val
	}

	if val, ok := metadata.Properties[collectionName]; ok && val != "" {
		meta.collectionName = val
	}

	if val, ok := metadata.Properties[writeConcern]; ok && val != "" {
		meta.writeconcern = val
	}

	if val, ok := metadata.Properties[readConcern]; ok && val != "" {
		meta.readconcern = val
	}

	if val, ok := metadata.Properties[params]; ok && val != "" {
		meta.params = val
	}

	var err error
	if val, ok := metadata.Properties[operationTimeout]; ok && val != "" {
		meta.operationTimeout, err = time.ParseDuration(val)
		if err != nil {
			return nil, errors.New("incorrect operationTimeout field from metadata")
		}
	}

	return &meta, nil
}

func getWriteConcernObject(cn string) (*writeconcern.WriteConcern, error) {
	var wc *writeconcern.WriteConcern
	if cn != "" {
		if cn == "majority" {
			wc = writeconcern.New(writeconcern.WMajority(), writeconcern.J(true), writeconcern.WTimeout(defaultTimeout))
		} else {
			w, err := strconv.Atoi(cn)
			wc = writeconcern.New(writeconcern.W(w), writeconcern.J(true), writeconcern.WTimeout(defaultTimeout))

			return wc, err
		}
	} else {
		wc = writeconcern.New(writeconcern.W(1), writeconcern.J(true), writeconcern.WTimeout(defaultTimeout))
	}

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
