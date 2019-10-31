// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mongodb

// mongodb package is an implementation of StateStore interface to perform operations on store

import (
	"context"
	"errors"
	"fmt"
	"time"

	json "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/state"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	host             = "host"
	username         = "username"
	password         = "password"
	databaseName     = "databaseName"
	collectionName   = "collectionName"
	operationTimeout = "operationTimeout"
	id               = "_id"
	value            = "value"

	defaultTimeout        = time.Duration(5 * time.Second)
	defaultDatabaseName   = "daprStore"
	defaultCollectionName = "daprCollection"

	// mongodb://<username>:<password@<host>/<database>
	connectionUriFormat = "mongodb://%s:%s@%s/%s"
)

// MongoDB is a state store implementation for MongoDB
type MongoDB struct {
	client           *mongo.Client
	collection       *mongo.Collection
	operationTimeout time.Duration
}

type mongoDBMetadata struct {
	host             string
	username         string
	password         string
	databaseName     string
	collectionName   string
	operationTimeout time.Duration
}

// Mongodb document wrapper
type Item struct {
	Key   string `bson:"_id"`
	Value string `bson:"value"`
}

// NewMongoDBStateStore returns a new MongoDB state store
func NewMongoDB() *MongoDB {
	return &MongoDB{}
}

// Init establishes connection to the store based on the metadata
func (m *MongoDB) Init(metadata state.Metadata) error {
	meta, err := getMongoDBMetaData(metadata)
	if err != nil {
		return err
	}

	client, err := getMongoDBClient(meta)

	if err != nil {
		return fmt.Errorf("error in creating mongodb client: %s", err)
	}

	m.client = client

	collection := m.client.Database(meta.databaseName).Collection(meta.collectionName)

	m.collection = collection

	return nil
}

// Set saves state into MongoDB
func (m *MongoDB) Set(req *state.SetRequest) error {
	var vStr string
	b, ok := req.Value.([]byte)
	if ok {
		vStr = string(b)
	} else {
		vStr, _ = json.MarshalToString(req.Value)
	}

	ctx, cancel := context.WithTimeout(context.Background(), m.operationTimeout)
	defer cancel()

	// create a document based on request key and value
	filter := bson.M{id: req.Key}
	update := bson.M{"$set": bson.M{id: req.Key, value: vStr}}
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
		return &state.GetResponse{}, err
	}

	value := []byte(result.Value)

	return &state.GetResponse{
		Data: value,
	}, nil
}

// BulkSet performs a bulks save operation
func (m *MongoDB) BulkSet(req []state.SetRequest) error {
	for _, s := range req {
		err := m.Set(&s)
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete performs a delete operation
func (m *MongoDB) Delete(req *state.DeleteRequest) error {

	ctx, cancel := context.WithTimeout(context.Background(), m.operationTimeout)
	defer cancel()

	filter := bson.M{id: req.Key}
	_, err := m.collection.DeleteOne(ctx, filter)

	if err != nil {
		return err
	}

	return nil
}

// BulkDelete performs a bulk delete operation
func (m *MongoDB) BulkDelete(req []state.DeleteRequest) error {
	for _, r := range req {
		err := m.Delete(&r)
		if err != nil {
			return err
		}
	}

	return nil
}

func getMongoDBClient(metadata *mongoDBMetadata) (*mongo.Client, error) {
	var uri string

	if metadata.username != "" && metadata.password != "" {
		uri = fmt.Sprintf(connectionUriFormat, metadata.username, metadata.password, metadata.host, metadata.databaseName)
	}

	// Set client options
	clientOptions := options.Client().ApplyURI(uri)

	// Connect to MongoDB
	ctx, _ := context.WithTimeout(context.Background(), defaultTimeout)

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
	} else {
		return nil, errors.New("missing or empty host field from metadata")
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

	var err error
	var t time.Duration
	if val, ok := metadata.Properties[operationTimeout]; ok && val != "" {
		t, err = time.ParseDuration(val)
	}

	if err != nil {
		return nil, errors.New("incorrect operationTimeout field from metadata")
	} else {
		meta.operationTimeout = t
	}

	return &meta, nil
}
