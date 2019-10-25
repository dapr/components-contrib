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
	"strings"
	"time"

	"github.com/dapr/components-contrib/state"
	jsoniter "github.com/json-iterator/go"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// constants for mongodb configuration
	hosts = "hosts"
)

type MongoDB struct {
	client           *mongo.Client
	databaseName     string
	collection       *mongo.Collection
	collectionName   string
	operationTimeout time.Duration
	json             jsoniter.API
}

type mongoDBMetadata struct {
	hosts        []string
	username     string
	password     string
	databaseName string
}

type Record struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NewMongoDBStateStore returns a new MongoDB state store
func NewMongoDBStateStore() *MongoDB {
	return &MongoDB{}
}

func (m *MongoDB) Init(metadata state.Metadata) error {
	meta, err := getMongoDBMetaData(metadata)
	if err != nil {
		return err
	}

	client, err := getMongoDBClient(meta)

	if err != nil {
		return fmt.Errorf("Error in creating mongodb client: %s", err)
	}

	m.client = client

	collection := m.client.Database(m.databaseName).Collection(m.collectionName)

	m.collection = collection

	return nil
}

// Set saves state into mongodb
func (m *MongoDB) Set(req *state.SetRequest) error {
	var value string
	b, ok := req.Value.([]byte)
	if ok {
		value = string(b)
	} else {
		value, _ = m.json.MarshalToString(req.Value)
	}

	ctx, _ := context.WithTimeout(context.Background(), m.operationTimeout)
	_, err := m.collection.InsertOne(ctx, bson.E{Key: req.Key, Value: value})

	if err != nil {
		return err
	}

	return nil
}

// Get retrieves state from MongoDB with a key
func (m *MongoDB) Get(req *state.GetRequest) (*state.GetResponse, error) {
	var result Record

	ctx, _ := context.WithTimeout(context.Background(), m.operationTimeout)

	ret := m.collection.FindOne(ctx, bson.E{Key: "Key", Value: req.Key})

	if ret == nil {
		return &state.GetResponse{}, errors.New("Document not found")
	}

	ret.Decode(&result)

	value, _ := m.json.Marshal(result.Value)

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

	ctx, _ := context.WithTimeout(context.Background(), m.operationTimeout)

	_, err := m.collection.DeleteOne(ctx, bson.E{Key: "Key", Value: req.Key})

	if err != nil {
		return fmt.Errorf("Error on deleting key: %s, error: %s", req.Key, err)
	}

	return nil
}

// BulkDelete performs a bulk delete operation
func (m *MongoDB) BulkDelete(req []state.DeleteRequest) error {
	for _, re := range req {
		err := m.Delete(&re)
		if err != nil {
			return err
		}
	}

	return nil
}

func getMongoDBClient(metadata *mongoDBMetadata) (*mongo.Client, error) {

	uriFormat := "mongodb+srv://%s:%s@%s"
	var uri string

	if metadata.username != "" && metadata.password != "" && len(metadata.hosts) != 0 {
		uri = fmt.Sprintf(uriFormat, metadata.username, metadata.password, metadata.hosts[0])
	}

	// Set client options
	clientOptions := options.Client().ApplyURI(uri)

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)

	if err != nil {
		return nil, err
	}

	return client, nil
}

func getMongoDBMetaData(metadata state.Metadata) (*mongoDBMetadata, error) {

	meta := mongoDBMetadata{}

	if val, ok := metadata.Properties[hosts]; ok && val != "" {
		meta.hosts = strings.Split(val, ",")
	} else {
		return nil, errors.New("missing or empty hosts field from metadata")
	}

	return &meta, nil
}
