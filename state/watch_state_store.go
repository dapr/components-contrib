// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

// WatchStateStore is an interface for initialization and support state watcher
type WatchStateStore interface {
	Init(metadata Metadata) error
	Watch(req *WatchStateRequest) (<-chan *StateEvent, error)
}
