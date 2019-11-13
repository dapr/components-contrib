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

package config

import "fmt"

const (
	// DefaultPrefetchCount is the default value for PrefetchCount().
	DefaultPrefetchCount = 100

	// DefaultPrefetchValidityMillis is the default value for PrefetchValidityMillis().
	DefaultPrefetchValidityMillis = 600000

	// MaximumPrefetchCount is the maximum value for prefetch count.
	// The reason to limit the prefetch count is that a single call to 'FlakeIDGenerator.NewID()` might
	// be blocked if the future allowance is exceeded: we want to avoid a single call for large batch to block
	// another call for small batch.
	MaximumPrefetchCount = 100000
)

// FlakeIDGeneratorConfig contains the configuration for 'FlakeIDGenerator' proxy.
type FlakeIDGeneratorConfig struct {
	name                   string
	prefetchCount          int32
	prefetchValidityMillis int64
}

// NewFlakeIDGeneratorConfig returns a new FlakeIDGeneratorConfig with the given name and default parameters.
func NewFlakeIDGeneratorConfig(name string) *FlakeIDGeneratorConfig {
	return &FlakeIDGeneratorConfig{
		name:                   name,
		prefetchCount:          DefaultPrefetchCount,
		prefetchValidityMillis: DefaultPrefetchValidityMillis,
	}
}

// NewFlakeIDGeneratorConfigWithParameters returns a new FlakeIDGeneratorConfig with the given name, prefetchCount and
// prefetchValidityMillis.
func NewFlakeIDGeneratorConfigWithParameters(name string, prefetchCount int32,
	prefetchValidityMillis int64) *FlakeIDGeneratorConfig {
	flakeCfg := &FlakeIDGeneratorConfig{
		name: name,
	}
	flakeCfg.SetPrefetchValidityMillis(prefetchValidityMillis)
	flakeCfg.SetPrefetchCount(prefetchCount)
	return flakeCfg
}

// SetPrefetchCount sets prefetchCount as the given value.
// prefetch count should be between 0 and MaximumPrefetchCount, otherwise it
// will panic.
func (igc *FlakeIDGeneratorConfig) SetPrefetchCount(prefetchCount int32) {
	if prefetchCount < 0 || prefetchCount > MaximumPrefetchCount {
		panic(fmt.Sprintf("prefectCount should be in the range of 0-%d", MaximumPrefetchCount))
	}
	igc.prefetchCount = prefetchCount
}

// SetName sets the name as the given name.
// Name must not be set after flake id config is added to config.
func (igc *FlakeIDGeneratorConfig) SetName(name string) {
	igc.name = name
}

// Name returns the name.
func (igc *FlakeIDGeneratorConfig) Name() string {
	return igc.name
}

// PrefetchCount returns the prefetchCount.
func (igc *FlakeIDGeneratorConfig) PrefetchCount() int32 {
	return igc.prefetchCount
}

// PrefetchValidityMillis returns the prefetchValidityMillis
func (igc *FlakeIDGeneratorConfig) PrefetchValidityMillis() int64 {
	return igc.prefetchValidityMillis
}

// SetPrefetchValidityMillis sets the prefetchValidityMillis as the given value.
func (igc *FlakeIDGeneratorConfig) SetPrefetchValidityMillis(prefetchValidityMillis int64) {
	if prefetchValidityMillis < 0 {
		panic(fmt.Sprintf("prefetchValidityMillis should be positive"))
	}
	igc.prefetchValidityMillis = prefetchValidityMillis
}
