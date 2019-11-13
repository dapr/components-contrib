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

// UsernamePasswordCredentials is a simple implementation of Credentials
// using username and password as security attributes.
type UsernamePasswordCredentials struct {
	*BaseCredentials
	username string
	password []byte
}

// NewUsernamePasswordCredentials returns UsernamePassWordCredentials with the given parameters.
func NewUsernamePasswordCredentials(username string, password string) *UsernamePasswordCredentials {
	//TODO :: Should we check if password is empty?
	return &UsernamePasswordCredentials{
		username: username,
		password: []byte(password),
		BaseCredentials: &BaseCredentials{
			principal: username,
		},
	}

}

// Password returns credentials password.
func (upc *UsernamePasswordCredentials) Password() string {
	return string(upc.password)
}

// Username returns credentials username.
func (upc *UsernamePasswordCredentials) Username() string {
	return upc.username
}

func (upc *UsernamePasswordCredentials) WritePortable(writer serialization.PortableWriter) (err error) {
	upc.BaseCredentials.WritePortable(writer)
	writer.WriteByteArray("pwd", upc.password)
	return
}

func (upc *UsernamePasswordCredentials) ReadPortable(reader serialization.PortableReader) (err error) {
	err = upc.BaseCredentials.ReadPortable(reader)
	if err != nil {
		return
	}
	upc.username = upc.BaseCredentials.principal
	upc.password = reader.ReadByteArray("pwd")
	return reader.Error()
}
