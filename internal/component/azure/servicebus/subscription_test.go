/*
Copyright 2022 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package servicebus

import (
	"context"
	"testing"

	"github.com/dapr/kit/logger"
)

var maxConcurrentHandlers = 100

func TestNewBulkSubscription_PrefetchValueShouldBeGreaterThanZero(t *testing.T) {
	testcases := []struct {
		name             string
		prefetchParam    int
		prefetchExpected int
	}{
		{
			"Prefetch passed is 0",
			0,
			1,
		},
		{
			"Prefetch passed is negative",
			-100,
			1,
		},
		{
			"Prefetch passed is positive",
			100,
			100,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			bulkSubscription := NewBulkSubscription(
				context.Background(),
				1000,
				1,
				tc.prefetchParam,
				10,
				&maxConcurrentHandlers,
				"test",
				logger.NewLogger("test"))
			if bulkSubscription.prefetch != tc.prefetchExpected {
				t.Errorf("Expected prefetch to be %d but got %d", tc.prefetchExpected, bulkSubscription.prefetch)
			}
		})
	}
}
