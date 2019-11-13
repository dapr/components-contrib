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
	clientAuthentication                  = 0x0002
	clientAuthenticationCustom            = 0x0003
	clientAddMembershipListener           = 0x0004
	clientCreateProxy                     = 0x0005
	clientDestroyProxy                    = 0x0006
	clientGetPartitions                   = 0x0008
	clientRemoveAllListeners              = 0x0009
	clientAddPartitionLostListener        = 0x000a
	clientRemovePartitionLostListener     = 0x000b
	clientGetDistributedObjects           = 0x000c
	clientAddDistributedObjectListener    = 0x000d
	clientRemoveDistributedObjectListener = 0x000e
	clientPing                            = 0x000f
	clientStatistics                      = 0x0010
	clientDeployClasses                   = 0x0011
	clientAddPartitionListener            = 0x0012
	clientCreateProxies                   = 0x0013
)
