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

package proto

const (
	replicatedmapPut                                = 0x0e01
	replicatedmapSize                               = 0x0e02
	replicatedmapIsEmpty                            = 0x0e03
	replicatedmapContainsKey                        = 0x0e04
	replicatedmapContainsValue                      = 0x0e05
	replicatedmapGet                                = 0x0e06
	replicatedmapRemove                             = 0x0e07
	replicatedmapPutAll                             = 0x0e08
	replicatedmapClear                              = 0x0e09
	replicatedmapAddEntryListenerToKeyWithPredicate = 0x0e0a
	replicatedmapAddEntryListenerWithPredicate      = 0x0e0b
	replicatedmapAddEntryListenerToKey              = 0x0e0c
	replicatedmapAddEntryListener                   = 0x0e0d
	replicatedmapRemoveEntryListener                = 0x0e0e
	replicatedmapKeySet                             = 0x0e0f
	replicatedmapValues                             = 0x0e10
	replicatedmapEntrySet                           = 0x0e11
	replicatedmapAddNearCacheEntryListener          = 0x0e12
)
