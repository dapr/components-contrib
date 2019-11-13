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

package versionutil

import (
	"strconv"
	"strings"
)

const (
	unknownVersion         int32 = -1
	majorVersionMultiplier int32 = 10000
	minorVersionMultiplier int32 = 100
)

func CalculateVersion(version string) int32 {
	if version == "" {
		return unknownVersion
	}
	mainParts := strings.Split(version, "-")
	tokens := strings.Split(mainParts[0], ".")

	if len(tokens) < 2 {
		return unknownVersion
	}

	majorCoeff, err := strconv.Atoi(tokens[0])
	if err != nil {
		return unknownVersion
	}
	minorCoeff, err := strconv.Atoi(tokens[1])

	if err != nil {
		return unknownVersion
	}
	calculatedVersion := int32(majorCoeff) * majorVersionMultiplier
	calculatedVersion += int32(minorCoeff) * minorVersionMultiplier

	if len(tokens) > 2 {
		lastCoeff, err := strconv.Atoi(tokens[2])
		if err != nil {
			return unknownVersion
		}
		calculatedVersion += int32(lastCoeff)
	}
	return calculatedVersion
}
