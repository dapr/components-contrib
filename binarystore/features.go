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

package binarystore

// Feature names an optional capability provided by a BinaryStore implementation.
// Implementations report their supported features via the Features() method.
type Feature string

// No features are defined for the initial release; this type is reserved for
// future extension (e.g. FeatureContentType, FeatureChecksumVerification).
