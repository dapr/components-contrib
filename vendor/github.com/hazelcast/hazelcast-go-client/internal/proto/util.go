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

package proto

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const ClientType = "GOO"

func dataCalculateSize(d serialization.Data) int {
	return len(d.Buffer()) + bufutil.Int32SizeInBytes
}

func stringCalculateSize(str string) int {
	return len(str) + bufutil.Int32SizeInBytes
}

func int64CalculateSize(v int64) int {
	return bufutil.Int64SizeInBytes
}

func addressCalculateSize(a *Address) int {
	dataSize := 0
	dataSize += stringCalculateSize(a.host)
	dataSize += bufutil.Int32SizeInBytes
	return dataSize
}
