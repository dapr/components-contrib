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

package property

import (
	"strconv"
	"time"
)

// HazelcastProperty is a struct for client properties.
type HazelcastProperty struct {
	name         string
	defaultValue string
	timeUnit     time.Duration
}

// NewHazelcastPropertyBool returns a Hazelcast property with the given defaultValue.
func NewHazelcastPropertyBool(name string, defaultValue bool) *HazelcastProperty {
	h := &HazelcastProperty{}
	h.name = name
	if defaultValue {
		h.defaultValue = "true"
	} else {
		h.defaultValue = "false"
	}
	return h
}

func NewHazelcastPropertyInt64WithTimeUnit(name string, defaultValue int64, timeUnit time.Duration) *HazelcastProperty {
	return &HazelcastProperty{
		name:         name,
		defaultValue: strconv.Itoa(int(defaultValue)),
		timeUnit:     timeUnit,
	}
}

// NewHazelcastProperty returns a Hazelcast property with the given name.
func NewHazelcastProperty(name string) *HazelcastProperty {
	return &HazelcastProperty{
		name: name,
	}
}

// NewHazelcastPropertyString returns a Hazelcast property with the given defaultValue.
func NewHazelcastPropertyString(name string, defaultValue string) *HazelcastProperty {
	return &HazelcastProperty{
		name:         name,
		defaultValue: defaultValue,
	}
}

// Name returns the name of this Hazelcast property.
func (h *HazelcastProperty) Name() string {
	return h.name
}

// SetName sets the name of this Hazelcast property as the given name.
func (h *HazelcastProperty) SetName(name string) {
	h.name = name
}

func (h *HazelcastProperty) String() string {
	return h.Name()
}

// TimeUnit returns the time unit for this property.
func (h *HazelcastProperty) TimeUnit() time.Duration {
	return h.timeUnit
}

// DefaultValue returns the default value of this Hazelcast property.
func (h *HazelcastProperty) DefaultValue() string {
	return h.defaultValue
}
