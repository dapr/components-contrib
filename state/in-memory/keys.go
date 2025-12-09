/*
Copyright 2025 The Dapr Authors
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

package inmemory

type sortingKeys struct {
	keys  []string
	items []*inMemStateStoreItem
}

func (s *sortingKeys) Len() int {
	return len(s.keys)
}

func (s *sortingKeys) Less(i, j int) bool {
	return s.items[i].idx < s.items[j].idx
}

func (s *sortingKeys) Swap(i, j int) {
	tmpk := s.keys[i]
	tmpi := s.items[i]
	s.keys[i] = s.keys[j]
	s.items[i] = s.items[j]
	s.keys[j] = tmpk
	s.items[j] = tmpi
}
