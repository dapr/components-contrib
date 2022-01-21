/*
Copyright 2021 The Dapr Authors
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

package local

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/kit/logger"
)

func TestNewResolver(t *testing.T) {
	resolver := NewResolver(logger.NewLogger("test"))
	assert.NotNil(t, resolver)
}

func TestRefreshCache(t *testing.T) {
	r := &resolver{
		logger:           logger.NewLogger("test"),
		processCache:     make(map[string]*procInfo),
		processCacheLock: &sync.Mutex{},
	}
	r.processCache["testAppID"] = &procInfo{
		processID: 4,
		grpcPort:  1024,
	}
	// the entry testAppID should be removed on cache refresh.
	r.refreshCache()

	_, ok := r.processCache["testAppID"]
	assert.False(t, ok)
}

func TestGetValue(t *testing.T) {
	r := &resolver{
		logger:           logger.NewLogger("test"),
		processCache:     make(map[string]*procInfo),
		processCacheLock: &sync.Mutex{},
	}
	r.processCache["testAppID"] = &procInfo{
		processID: 4,
		grpcPort:  1024,
	}

	val, ok := r.getValue("testAppID")
	assert.True(t, ok)
	assert.Equal(t, int32(4), val.processID)
	assert.Equal(t, 1024, val.grpcPort)
}
