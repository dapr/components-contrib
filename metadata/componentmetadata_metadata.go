//go:build metadata
// +build metadata

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

package metadata

// ComponentWithMetadata includes the GetComponentMetadata method when the `metadata` build tag is present.
// The build tag is present when running the linter.
type ComponentWithMetadata interface {
	GetComponentMetadata() MetadataMap
}
