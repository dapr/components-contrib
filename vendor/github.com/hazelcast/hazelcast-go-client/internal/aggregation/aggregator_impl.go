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

package aggregation

import (
	"github.com/hazelcast/hazelcast-go-client/internal/util/precond"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type aggregator struct {
	id int32
}

func (ag *aggregator) WriteData(output serialization.DataOutput) (err error) {
	return
}

func (ag *aggregator) ReadData(input serialization.DataInput) (err error) {
	return
}

func (*aggregator) FactoryID() int32 {
	return FactoryID
}

func (ag *aggregator) ClassID() int32 {
	return ag.id
}

func newAggregator(id int32) *aggregator {
	return &aggregator{id: id}
}

type Count struct {
	*aggregator
	attributePath string
}

func NewCount(attributePath string) (*Count, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &Count{aggregator: newAggregator(count), attributePath: attributePath}, nil
}

func (c *Count) ReadData(input serialization.DataInput) (err error) {
	c.aggregator = newAggregator(count)
	c.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadInt64()
	return input.Error()
}

func (c *Count) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(c.attributePath)
	// member side field, not used in client
	output.WriteInt64(0)
	return
}

type Float64Average struct {
	*aggregator
	attributePath string
}

func NewFloat64Average(attributePath string) (*Float64Average, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &Float64Average{aggregator: newAggregator(float64Avg), attributePath: attributePath}, nil
}

func (f *Float64Average) ReadData(input serialization.DataInput) (err error) {
	f.aggregator = newAggregator(float64Avg)
	f.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadFloat64()
	input.ReadInt64()
	return input.Error()
}

func (f *Float64Average) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(f.attributePath)
	// member side field, not used in client
	output.WriteFloat64(0)
	output.WriteInt64(0)
	return
}

type Float64Sum struct {
	*aggregator
	attributePath string
}

func NewFloat64Sum(attributePath string) (*Float64Sum, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &Float64Sum{aggregator: newAggregator(float64Sum), attributePath: attributePath}, nil
}

func (f *Float64Sum) ReadData(input serialization.DataInput) (err error) {
	f.aggregator = newAggregator(float64Sum)
	f.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadFloat64()
	input.ReadFloat64()
	return input.Error()
}

func (f *Float64Sum) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(f.attributePath)
	// member side field, not used in client
	output.WriteFloat64(0)
	output.WriteFloat64(0)
	return
}

type FixedPointSum struct {
	*aggregator
	attributePath string
}

func NewFixedPointSum(attributePath string) (*FixedPointSum, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &FixedPointSum{aggregator: newAggregator(fixedPointSum), attributePath: attributePath}, nil
}

func (f *FixedPointSum) ReadData(input serialization.DataInput) (err error) {
	f.aggregator = newAggregator(fixedPointSum)
	f.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadInt64()
	return input.Error()
}

func (f *FixedPointSum) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(f.attributePath)
	// member side field, not used in client
	output.WriteInt64(0)
	return
}

type FloatingPointSum struct {
	*aggregator
	attributePath string
}

func NewFloatingPointSum(attributePath string) (*FloatingPointSum, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &FloatingPointSum{aggregator: newAggregator(floatingPointSum), attributePath: attributePath}, nil
}

func (f *FloatingPointSum) ReadData(input serialization.DataInput) (err error) {
	f.aggregator = newAggregator(floatingPointSum)
	f.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadFloat64()
	return input.Error()
}

func (f *FloatingPointSum) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(f.attributePath)
	// member side field, not used in client
	output.WriteFloat64(0)
	return
}

type Max struct {
	*aggregator
	attributePath string
}

func NewMax(attributePath string) (*Max, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &Max{aggregator: newAggregator(max), attributePath: attributePath}, nil
}

func (m *Max) ReadData(input serialization.DataInput) (err error) {
	m.aggregator = newAggregator(max)
	m.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadObject()
	return input.Error()
}

func (m *Max) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(m.attributePath)
	// member side field, not used in client
	output.WriteObject(nil)
	return
}

type Min struct {
	*aggregator
	attributePath string
}

func NewMin(attributePath string) (*Min, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &Min{aggregator: newAggregator(min), attributePath: attributePath}, nil
}

func (m *Min) ReadData(input serialization.DataInput) (err error) {
	m.aggregator = newAggregator(min)
	m.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadObject()
	return input.Error()
}

func (m *Min) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(m.attributePath)
	// member side field, not used in client
	output.WriteObject(nil)
	return
}

type Int32Average struct {
	*aggregator
	attributePath string
}

func NewInt32Average(attributePath string) (*Int32Average, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &Int32Average{aggregator: newAggregator(int32Avg), attributePath: attributePath}, nil
}

func (i *Int32Average) ReadData(input serialization.DataInput) (err error) {
	i.aggregator = newAggregator(int32Avg)
	i.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadInt64()
	input.ReadInt64()
	return input.Error()
}

func (i *Int32Average) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(i.attributePath)
	// member side field, not used in client
	output.WriteInt64(0)
	output.WriteInt64(0)
	return
}

type Int32Sum struct {
	*aggregator
	attributePath string
}

func NewInt32Sum(attributePath string) (*Int32Sum, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &Int32Sum{aggregator: newAggregator(int32Sum), attributePath: attributePath}, nil
}

func (i *Int32Sum) ReadData(input serialization.DataInput) (err error) {
	i.aggregator = newAggregator(int32Sum)
	i.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadInt64()
	input.ReadInt64()
	return input.Error()
}

func (i *Int32Sum) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(i.attributePath)
	// member side field, not used in client
	output.WriteInt64(0)
	output.WriteInt64(0)
	return
}

type Int64Average struct {
	*aggregator
	attributePath string
}

func NewInt64Average(attributePath string) (*Int64Average, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &Int64Average{aggregator: newAggregator(int64Avg), attributePath: attributePath}, nil
}

func (i *Int64Average) ReadData(input serialization.DataInput) (err error) {
	i.aggregator = newAggregator(int64Avg)
	i.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadInt64()
	input.ReadInt64()
	return input.Error()
}

func (i *Int64Average) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(i.attributePath)
	// member side field, not used in client
	output.WriteInt64(0)
	output.WriteInt64(0)
	return
}

type Int64Sum struct {
	*aggregator
	attributePath string
}

func NewInt64Sum(attributePath string) (*Int64Sum, error) {
	err := precond.CheckHasText(attributePath, "attributePath should not be empty")
	if err != nil {
		return nil, err
	}
	return &Int64Sum{aggregator: newAggregator(int64Sum), attributePath: attributePath}, nil
}

func (i *Int64Sum) ReadData(input serialization.DataInput) (err error) {
	i.aggregator = newAggregator(int64Sum)
	i.attributePath = input.ReadUTF()
	// member side field, not used in client
	input.ReadInt64()
	input.ReadInt64()
	return input.Error()
}

func (i *Int64Sum) WriteData(output serialization.DataOutput) (err error) {
	output.WriteUTF(i.attributePath)
	// member side field, not used in client
	output.WriteInt64(0)
	output.WriteInt64(0)
	return
}
