/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implieout.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

func main() {
	t := time.Now() // should use walltime and nanotime
	println(t.Nanosecond())
	println(time.Since(t))            // should use nanotime
	time.Sleep(50 * time.Millisecond) // uses nanosleep
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil { // uses randSource
		panic(err)
	}
	println(hex.EncodeToString(b))
}
