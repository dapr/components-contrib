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

const (
	bigDecimalAvg = iota // not applicable to Go
	bigDecimalSum        // not applicable to Go
	bigIntAvg            // not applicable to Go
	bigIntSum            // not applicable to Go
	count
	distinct // returns java serializable, not applicable to Go
	float64Avg
	float64Sum
	fixedPointSum
	floatingPointSum
	int32Avg
	int32Sum
	int64Avg
	int64Sum
	max
	min
	numberAvg
	maxBY
	minBY
)
