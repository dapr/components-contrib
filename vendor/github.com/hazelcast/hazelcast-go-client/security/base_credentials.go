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

package security

import "github.com/hazelcast/hazelcast-go-client/serialization"

const (
	factoryID = 1
	classID   = 1
)

// BaseCredentials is base implementation for Credentials interface.
type BaseCredentials struct {
	endpoint  string
	principal string
}

func (bc *BaseCredentials) FactoryID() int32 {
	return factoryID
}

func (bc *BaseCredentials) ClassID() int32 {
	return classID
}

func (bc *BaseCredentials) WritePortable(writer serialization.PortableWriter) (err error) {
	writer.WriteUTF("principal", bc.principal)
	writer.WriteUTF("endpoint", bc.endpoint)
	return nil
}

func (bc *BaseCredentials) ReadPortable(reader serialization.PortableReader) (err error) {
	bc.endpoint = reader.ReadUTF("endpoint")
	bc.principal = reader.ReadUTF("principal")
	return reader.Error()
}

func (bc *BaseCredentials) Endpoint() string {
	return bc.endpoint
}

func (bc *BaseCredentials) SetEndpoint(endpoint string) {
	bc.endpoint = endpoint
}

func (bc *BaseCredentials) Principal() string {
	return bc.principal
}
