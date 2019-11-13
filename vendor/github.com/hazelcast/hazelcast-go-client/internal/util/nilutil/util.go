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
// limitations under

package nilutil

import "reflect"

// IsNil does proper nil check for interface{} values taken from users
// "== nil" check for interfaces are not enough
//when an nil pointer is assigned to interface, it returns false from "== nil" check
// Example:
//var pnt *int
//var inf interface{}
//inf = pnt
//fmt.Println(inf == nil) // false
//fmt.Println(isNil(inf)) // true
func IsNil(arg interface{}) bool {
	return arg == nil || (reflect.ValueOf(arg).Kind() == reflect.Ptr) && reflect.ValueOf(arg).IsNil()
}
