/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package daprmqtest is a fake DaprMQ REST API for testing the DaprMQ pub/sub components.
package daprmqtest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Fake answers the DaprMQ topic and queue routes the components use. Lock its fields with Snapshot.
type Fake struct {
	mu sync.Mutex

	Published   map[string][]json.RawMessage // topic -> published items
	Priorities  []int                        // priority of every published or enqueued item
	Subscribers map[string]int               // "topic/subscriber" -> subscribe calls
	Queues      map[string][]json.RawMessage // queue -> items waiting
	Acked       []string
	Nacked      []string
	DeadLetters []string
	Extends     []ExtendCall
	ExtendsLost bool // extend-lock answers LOCK_NOT_FOUND
	Dequeues    map[string]int

	nextLock int
}

type ExtendCall struct {
	LockID               string `json:"lockId"`
	AdditionalTTLSeconds int    `json:"additionalTtlSeconds"`
}

func New() *Fake {
	return &Fake{
		Published:   map[string][]json.RawMessage{},
		Subscribers: map[string]int{},
		Queues:      map[string][]json.RawMessage{},
		Dequeues:    map[string]int{},
	}
}

// Enqueue adds an item to the back of a queue.
func (f *Fake) Enqueue(queueID string, item json.RawMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Queues[queueID] = append(f.Queues[queueID], item)
}

// Snapshot calls read with the fake locked.
func (f *Fake) Snapshot(read func(f *Fake)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	read(f)
}

// TotalDequeues is the number of dequeue calls across all queues.
func (f *Fake) TotalDequeues() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, n := range f.Dequeues {
		total += n
	}
	return total
}

func (f *Fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	body, _ := io.ReadAll(r.Body)
	reply := func(status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	var items struct {
		Items []struct {
			Item     json.RawMessage `json:"item"`
			Priority int             `json:"priority"`
		} `json:"items"`
	}

	switch {
	case parts[0] == "topic" && parts[2] == "publish":
		_ = json.Unmarshal(body, &items)
		for _, it := range items.Items {
			f.Published[parts[1]] = append(f.Published[parts[1]], it.Item)
			f.Priorities = append(f.Priorities, it.Priority)
		}
		reply(http.StatusAccepted, map[string]any{"accepted": true, "publishId": "p", "sequence": 1})

	case parts[0] == "topic" && parts[2] == "subscribers":
		key := parts[1] + "/" + parts[3]
		f.Subscribers[key]++
		if f.Subscribers[key] > 1 {
			reply(http.StatusConflict, map[string]any{"message": "Subscriber already exists", "success": false})
			return
		}
		reply(http.StatusCreated, map[string]any{"success": true, "queueActorId": parts[1] + "-sub-" + parts[3]})

	case parts[0] == "queue" && parts[2] == "enqueue":
		_ = json.Unmarshal(body, &items)
		for _, it := range items.Items {
			f.Queues[parts[1]] = append(f.Queues[parts[1]], it.Item)
			f.Priorities = append(f.Priorities, it.Priority)
		}
		reply(http.StatusOK, map[string]any{"success": true, "itemsEnqueued": len(items.Items)})

	case parts[0] == "queue" && parts[2] == "dequeue":
		f.Dequeues[parts[1]]++
		q := f.Queues[parts[1]]
		if len(q) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		item := q[0]
		f.Queues[parts[1]] = q[1:]
		f.nextLock++
		lockID := fmt.Sprintf("lock-%d", f.nextLock)
		ttl, _ := strconv.Atoi(r.Header.Get("ttl-seconds"))
		expiresAt := time.Now().Unix() + int64(ttl)
		reply(http.StatusOK, map[string]any{"items": []map[string]any{{"item": item, "priority": 1, "lockId": lockID, "lockExpiresAt": expiresAt}}})

	case parts[0] == "queue":
		var req struct {
			LockID string `json:"lockId"`
		}
		_ = json.Unmarshal(body, &req)
		switch parts[2] {
		case "extend-lock":
			var call ExtendCall
			_ = json.Unmarshal(body, &call)
			f.Extends = append(f.Extends, call)
			if f.ExtendsLost {
				reply(http.StatusNotFound, map[string]any{"success": false, "message": "Lock not found", "errorCode": "LOCK_NOT_FOUND"})
				return
			}
			reply(http.StatusOK, map[string]any{"success": true, "newExpiresAt": 0})
		case "acknowledge":
			f.Acked = append(f.Acked, req.LockID)
			reply(http.StatusOK, map[string]any{"success": true})
		case "nack":
			f.Nacked = append(f.Nacked, req.LockID)
			reply(http.StatusOK, map[string]any{"success": true, "deliveryCount": 1})
		case "deadletter":
			f.DeadLetters = append(f.DeadLetters, req.LockID)
			reply(http.StatusOK, map[string]any{"success": true})
		}

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}
