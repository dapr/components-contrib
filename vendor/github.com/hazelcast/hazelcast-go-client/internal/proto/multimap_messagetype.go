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
	multimapPut                   = 0x0201
	multimapGet                   = 0x0202
	multimapRemove                = 0x0203
	multimapKeySet                = 0x0204
	multimapValues                = 0x0205
	multimapEntrySet              = 0x0206
	multimapContainsKey           = 0x0207
	multimapContainsValue         = 0x0208
	multimapContainsEntry         = 0x0209
	multimapSize                  = 0x020a
	multimapClear                 = 0x020b
	multimapValueCount            = 0x020c
	multimapAddEntryListenerToKey = 0x020d
	multimapAddEntryListener      = 0x020e
	multimapRemoveEntryListener   = 0x020f
	multimapLock                  = 0x0210
	multimapTryLock               = 0x0211
	multimapIsLocked              = 0x0212
	multimapUnlock                = 0x0213
	multimapForceUnlock           = 0x0214
	multimapRemoveEntry           = 0x0215
	multimapDelete                = 0x0216
)
