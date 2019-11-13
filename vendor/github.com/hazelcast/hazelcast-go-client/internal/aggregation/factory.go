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

import "github.com/hazelcast/hazelcast-go-client/serialization"

const FactoryID = -41

type Factory struct {
}

func NewFactory() *Factory {
	return &Factory{}
}

func (*Factory) Create(id int32) serialization.IdentifiedDataSerializable {
	switch id {
	case count:
		return &Count{}
	case float64Avg:
		return &Float64Average{}
	case float64Sum:
		return &Float64Sum{}
	case fixedPointSum:
		return &FixedPointSum{}
	case floatingPointSum:
		return &FloatingPointSum{}
	case int32Avg:
		return &Int32Average{}
	case int32Sum:
		return &Int32Sum{}
	case int64Avg:
		return &Int64Average{}
	case int64Sum:
		return &Int64Sum{}
	case max:
		return &Max{}
	case min:
		return &Min{}
	default:
		return nil
	}
}
