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
	"github.com/dapr/kit/ptr"
)

func TestNewBulkSubscription_MaxBulkSubCountShouldBeGreaterThanZero(t *testing.T) {
	testcases := []struct {
		name                    string
		maxBulkSubCountParam    int
		maxBulkSubCountExpected int
	}{
		{
			"maxBulkSubCount passed is 0",
			0,
			1,
		},
		{
			"maxBulkSubCount passed is negative",
			-100,
			1,
		},
		{
			"maxBulkSubCount passed is positive",
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
				tc.maxBulkSubCountParam,
				10,
				ptr.Of(100),
				"test",
				logger.NewLogger("test"))
			if bulkSubscription.maxBulkSubCount != tc.maxBulkSubCountExpected {
				t.Errorf("Expected maxBulkSubCount to be %d but got %d", tc.maxBulkSubCountExpected, bulkSubscription.maxBulkSubCount)
			}
		})
	}
}
