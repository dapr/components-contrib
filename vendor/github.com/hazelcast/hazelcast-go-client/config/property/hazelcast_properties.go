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
	"os"

	"strings"

	"time"

	"strconv"

	"fmt"

	"github.com/hazelcast/hazelcast-go-client/config"
)

// HazelcastProperties is a container for configured Hazelcast properties.
// A Hazelcast property can be set via:
//  * an environmental variable
//  * programmatic configuration Config.SetProperty
type HazelcastProperties struct {
	properties config.Properties
}

// NewHazelcastProperties returns HazelcastProperties with the given properties.
func NewHazelcastProperties(properties config.Properties) *HazelcastProperties {
	hp := &HazelcastProperties{}
	hp.properties = make(config.Properties)
	if properties != nil {
		hp.properties = properties
	}
	return hp
}

// GetString returns the value for the given property.
// It first checks config, then environment variables, and lastly
// if it is not found in those it returns the default value.
func (hp *HazelcastProperties) GetString(property *HazelcastProperty) string {
	// First check if property exists in config
	if prop, found := hp.properties[property.Name()]; found {
		return prop
	}
	// Check if exists in environment variables
	if prop, found := os.LookupEnv(property.Name()); found {
		return prop
	}
	return property.DefaultValue()

}

// GetBoolean returns the boolean value of the given property.
// It returns true if the value is equal to "true" (case-insensitive).
func (hp *HazelcastProperties) GetBoolean(property *HazelcastProperty) bool {
	str := hp.GetString(property)
	return strings.Compare(strings.ToLower(str), "true") == 0
}

// GetDuration returns the time duration from the given property.
// It returns the default value if the set time duration cannot be parsed.
// It panics if the set and default values cannot be parsed to time.Duration.
func (hp *HazelcastProperties) GetDuration(property *HazelcastProperty) time.Duration {
	str := hp.GetString(property)
	if _, err := strconv.Atoi(str); err != nil {
		str = property.DefaultValue()
		if _, err = strconv.Atoi(str); err != nil {
			panic(fmt.Sprintf("%s cannot be parsed to int", str))
		}
	}
	// If we come to this line, there cannot be any error during conversion of str
	coeff, _ := strconv.Atoi(str)
	duration := time.Duration(coeff) * property.TimeUnit()
	return duration
}

// GetPositiveDurationOrDef returns the time duration as GetDuration except it returns the default value
// if the set time duration is a non-positive value.
func (hp *HazelcastProperties) GetPositiveDurationOrDef(property *HazelcastProperty) time.Duration {
	duration := hp.GetDuration(property)
	if duration <= 0 {
		defaultValInt, _ := strconv.Atoi(property.DefaultValue())
		duration = time.Duration(defaultValInt) * property.TimeUnit()
	}
	return duration
}
