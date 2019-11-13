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
	listSize                = 0x0501
	listContains            = 0x0502
	listContainsAll         = 0x0503
	listAdd                 = 0x0504
	listRemove              = 0x0505
	listAddAll              = 0x0506
	listCompareAndRemoveAll = 0x0507
	listCompareAndRetainAll = 0x0508
	listClear               = 0x0509
	listGetAll              = 0x050a
	listAddListener         = 0x050b
	listRemoveListener      = 0x050c
	listIsEmpty             = 0x050d
	listAddAllWithIndex     = 0x050e
	listGet                 = 0x050f
	listSet                 = 0x0510
	listAddWithIndex        = 0x0511
	listRemoveWithIndex     = 0x0512
	listLastIndexOf         = 0x0513
	listIndexOf             = 0x0514
	listSub                 = 0x0515
	listIterator            = 0x0516
	listListIterator        = 0x0517
)
