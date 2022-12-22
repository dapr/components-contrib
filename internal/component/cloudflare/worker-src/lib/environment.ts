/*
Copyright 2022 The Dapr Authors
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

export type Environment = {
    // PEM-encoded Ed25519 public key used to verify JWT tokens
    PUBLIC_KEY: string
    // Audience for the token - this is normally the worker's name
    TOKEN_AUDIENCE: string
    // Skips authorization - used for development
    SKIP_AUTH: string
    // Other values are assumed to be bindings: Queues, KV, R2
    readonly [x: string]: string | Queue<string> | KVNamespace | R2Bucket
}
