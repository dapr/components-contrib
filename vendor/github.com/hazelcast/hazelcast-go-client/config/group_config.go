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

// GroupConfig contains the configuration for Hazelcast groups.
// With groups it is possible to create multiple clusters where each cluster has its own group and doesn't
// interfere with other clusters.
type GroupConfig struct {
	// the group name of the group.
	name string

	// the group password of the group.
	password string
}

// NewGroupConfig returns a new GroupConfig with default group name and password.
func NewGroupConfig() *GroupConfig {
	return &GroupConfig{name: defaultGroupName, password: defaultGroupPassword}
}

// Name returns the group name of the group.
func (gc *GroupConfig) Name() string {
	return gc.name
}

// Password returns the group password of the group.
func (gc *GroupConfig) Password() string {
	return gc.password
}

// SetName sets the group name of the group.
func (gc *GroupConfig) SetName(name string) {
	gc.name = name
}

// SetPassword sets the group password of the group.
// SetPassword returns the configured GroupConfig for chaining.
func (gc *GroupConfig) SetPassword(password string) *GroupConfig {
	gc.password = password
	return gc
}
