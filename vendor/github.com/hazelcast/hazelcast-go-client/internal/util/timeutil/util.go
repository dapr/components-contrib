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

package timeutil

import (
	"math"
	"time"
)

func GetTimeInMilliSeconds(duration time.Duration) int64 {
	if duration == -1 {
		return -1
	}
	// if duration is positive and less than a millisecond return 1.
	if duration > 0 && duration < time.Millisecond {
		return 1
	}
	return duration.Nanoseconds() / int64(time.Millisecond)
}

func GetCurrentTimeInMilliSeconds() int64 {
	return time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

func ConvertMillisToDuration(timeInMillis int64) time.Duration {
	if timeInMillis == math.MaxInt64 {
		return time.Duration(timeInMillis)
	}
	return time.Duration(timeInMillis) * time.Millisecond
}

func ConvertMillisToUnixTime(timeInMillis int64) time.Time {
	if timeInMillis == 0 {
		return time.Time{}
	} else if timeInMillis == math.MaxInt64 {
		return time.Unix(0, timeInMillis)
	}
	return time.Unix(0, timeInMillis*int64(time.Millisecond))
}

func GetPositiveDurationOrMax(duration time.Duration) time.Duration {
	if duration > 0 {
		return duration
	}
	return time.Duration(math.MaxInt64)
}

func GetCurrentTimeInMillis() int64 {
	now := time.Now()
	unixNano := now.UnixNano()
	return unixNano / 1000000
}
