/*
Copyright 2021 The Dapr Authors
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

package bearer

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	_ AllowlistMatcher = (*exactMatcher)(nil)

	ErrUnimplemented = errors.New("unimplemented match type")
)

type AllowlistMatcher interface {
	Match(path string) bool
}

func NewMatcher(matchType string, match string) (AllowlistMatcher, error) {
	switch matchType {
	case "":
		fallthrough
	case matchTypeExact:
		return newExactMatcher(match), nil
	case matchTypeRegex:
		return newRegexMatcher(match)
	default:
		return nil, ErrUnimplemented
	}
}

type exactMatcher struct {
	Path []string
}

func newExactMatcher(match string) (m *exactMatcher) {
	m = &exactMatcher{}
	if len(match) == 0 {
		return
	}
	m.Path = strings.Split(match, allowlistSeparator)
	return
}

func (w *exactMatcher) Match(path string) bool {
	if len(w.Path) == 0 {
		return false
	}
	return InArray(path, w.Path)
}

func InArray(in string, array []string) bool {
	for k := range array {
		if in == array[k] {
			return true
		}
	}
	return false
}

type regexMatcher struct {
	Path []*regexp.Regexp
}

func newRegexMatcher(match string) (m *regexMatcher, err error) {
	m = &regexMatcher{}
	if len(match) == 0 {
		return
	}
	paths := strings.Split(match, allowlistSeparator)
	m.Path = make([]*regexp.Regexp, 0, len(paths))
	for _, path := range paths {
		reg, err := regexp.Compile(path)
		if err != nil {
			return nil, fmt.Errorf("compile regex err: %matcher", err)
		}
		m.Path = append(m.Path, reg)
	}
	return
}

func (w *regexMatcher) Match(path string) bool {
	if len(w.Path) == 0 {
		return false
	}
	for _, reg := range w.Path {
		if reg.MatchString(path) {
			return true
		}
	}
	return false
}
