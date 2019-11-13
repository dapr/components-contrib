// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spi

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/internal"
)

// NewSerializationService creates and returns a new serialization service with the given config.
func NewSerializationService(serializationConfig *serialization.Config) (SerializationService, error) {
	return internal.NewService(serializationConfig)
}

// NewData return serialization Data with the given payload.
func NewData(payload []byte) serialization.Data {
	return internal.NewData(payload)
}

// SerializationService is used to serialize user objects to Data and deserialize data to objects.
type SerializationService interface {
	// ToObject deserializes the given data to an object.
	// It can safely be called on an object that is already deserialized. In that case, that instance
	// is returned.
	// If this is called with nil, nil is returned.
	ToObject(data serialization.Data) (interface{}, error)
	// ToData serializes an object to a data.
	// It can safely be called with a Data. In that case, that instance is returned.
	// If it is called with nil, nil is returned.
	ToData(object interface{}) (serialization.Data, error)
}
