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
	mapPut                                = 0x0101
	mapGet                                = 0x0102
	mapRemove                             = 0x0103
	mapReplace                            = 0x0104
	mapReplaceIfSame                      = 0x0105
	mapContainsKey                        = 0x0109
	mapContainsValue                      = 0x010a
	mapRemoveIfSame                       = 0x010b
	mapDelete                             = 0x010c
	mapFlush                              = 0x010d
	mapTryRemove                          = 0x010e
	mapTryPut                             = 0x010f
	mapPutTransient                       = 0x0110
	mapPutIfAbsent                        = 0x0111
	mapSet                                = 0x0112
	mapLock                               = 0x0113
	mapTryLock                            = 0x0114
	mapIsLocked                           = 0x0115
	mapUnlock                             = 0x0116
	mapAddInterceptor                     = 0x0117
	mapRemoveInterceptor                  = 0x0118
	mapAddEntryListenerToKeyWithPredicate = 0x0119
	mapAddEntryListenerWithPredicate      = 0x011a
	mapAddEntryListenerToKey              = 0x011b
	mapAddEntryListener                   = 0x011c
	mapAddNearCacheEntryListener          = 0x011d
	mapRemoveEntryListener                = 0x011e
	mapAddPartitionLostListener           = 0x011f
	mapRemovePartitionLostListener        = 0x0120
	mapGetEntryView                       = 0x0121
	mapEvict                              = 0x0122
	mapEvictAll                           = 0x0123
	mapLoadAll                            = 0x0124
	mapLoadGivenKeys                      = 0x0125
	mapKeySet                             = 0x0126
	mapGetAll                             = 0x0127
	mapValues                             = 0x0128
	mapEntrySet                           = 0x0129
	mapKeySetWithPredicate                = 0x012a
	mapValuesWithPredicate                = 0x012b
	mapEntriesWithPredicate               = 0x012c
	mapAddIndex                           = 0x012d
	mapSize                               = 0x012e
	mapIsEmpty                            = 0x012f
	mapPutAll                             = 0x0130
	mapClear                              = 0x0131
	mapExecuteOnKey                       = 0x0132
	mapSubmitToKey                        = 0x0133
	mapExecuteOnAllKeys                   = 0x0134
	mapExecuteWithPredicate               = 0x0135
	mapExecuteOnKeys                      = 0x0136
	mapForceUnlock                        = 0x0137
	mapKeySetWithPagingPredicate          = 0x0138
	mapValuesWithPagingPredicate          = 0x0139
	mapEntriesWithPagingPredicate         = 0x013a
	mapClearNearCache                     = 0x013b
	mapFetchKeys                          = 0x013c
	mapFetchEntries                       = 0x013d
	mapAggregate                          = 0x013e
	mapAggregateWithPredicate             = 0x013f
	mapProject                            = 0x0140
	mapProjectWithPredicate               = 0x0141
	mapFetchNearCacheInvalidationMetadata = 0x0142
	mapAssignAndGetUuids                  = 0x0143
	mapRemoveAll                          = 0x0144
	mapAddNearCacheInvalidationListener   = 0x0145
	mapFetchWithQuery                     = 0x0146
	mapEventJournalSubscribe              = 0x0147
	mapEventJournalRead                   = 0x0148
)
