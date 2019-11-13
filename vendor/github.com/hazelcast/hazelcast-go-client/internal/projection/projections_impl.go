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

package projection

import (
	"github.com/hazelcast/hazelcast-go-client/internal/util/precond"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const singleAttributeProjectionID = 0

type SingleAttribute struct {
	attributePath string
}

func NewSingleAttribute(attributePath string) (*SingleAttribute, error) {
	err := precond.CheckHasText(attributePath, "attributePath must not be empty")
	if err != nil {
		return nil, err
	}
	return &SingleAttribute{attributePath}, nil
}

func (*SingleAttribute) FactoryID() (factoryID int32) {
	return FactoryID
}

func (*SingleAttribute) ClassID() (classID int32) {
	return singleAttributeProjectionID
}

func (sa *SingleAttribute) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(sa.attributePath)
	return
}

func (sa *SingleAttribute) ReadData(input serialization.DataInput) error {
	sa.attributePath = input.ReadUTF()
	return input.Error()
}
