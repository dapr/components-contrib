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

package core

// Cluster is a cluster service for Hazelcast clients.
// It provides access to the members in the cluster and one can register for changes in the
// cluster members.
// All the methods on the Cluster are thread-safe.
type Cluster interface {
	// AddMembershipListener registers the given listener to listen to membership updates.
	// AddMembershipListener returns uuid which will be used to remove the listener.
	// There is no check for duplicate registrations, so if you register the listener twice,
	// it will get events twice.
	// The given listener should implement MemberAddedListener or MemberRemovedListener interfaces or both.
	// If the given listener does not implement any of these, it will not have any effect.
	AddMembershipListener(listener interface{}) string

	// RemoveMembershipListener removes the listener with the given registrationID.
	// RemoveMembershipListener returns true if successfully removed, false otherwise.
	// If the same MembershipListener is registered multiple times,
	// it needs to be removed multiple times.
	RemoveMembershipListener(registrationID string) bool

	// GetMembers returns a slice of current members in the cluster. The returned slice is
	// a copy of current members.
	GetMembers() []Member
}
