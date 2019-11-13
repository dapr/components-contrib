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

// Topic is a distribution mechanism for publishing messages that are delivered to multiple subscribers, which
// is also known as a publish/subscribe (pub/sub) messaging model. Publish and subscriptions are cluster-wide. When a
// member subscribes for a topic, it is actually registering for messages published by any member in the cluster,
// including the new members joined after you added the listener.
//
// Messages are ordered, meaning that listeners(subscribers) will process the messages in the order they are actually
// published.
type Topic interface {
	// DistributedObject is the base interface for all distributed objects.
	DistributedObject

	// AddMessageListener subscribes to this topic. When someone publishes a message on this topic.
	// OnMessage() function of the given messageListener is called. More than one message listener can be
	// added on one instance.
	// AddMessageListener returns registrationID of the listener.
	AddMessageListener(messageListener MessageListener) (registrationID string, err error)

	// RemoveMessageListener stops receiving messages for the listener with the given registrationID.
	// If the listener is already removed, this method does nothing.
	// RemoveMessageListener returns true if the listener with the given registrationID is removed.
	RemoveMessageListener(registrationID string) (removed bool, err error)

	// Publish publishes the message to all subscribers of this topic.
	Publish(message interface{}) (err error)
}
