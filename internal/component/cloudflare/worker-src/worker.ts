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

import { Router, type Request as RequestI } from 'itty-router'

import type { Environment } from '$lib/environment'
import { AuthorizeRequest } from '$lib/jwt-auth'
import { version } from './package.json'

const router = Router()
    // Handle the info endpoint
    .get(
        '/.well-known/dapr/info',
        async (
            req: Request & RequestI,
            env: Environment
        ): Promise<Response> => {
            const auth = await AuthorizeRequest(req, env)
            if (!auth) {
                return new Response('Unauthorized', { status: 401 })
            }

            // Filter all bindings by type
            const queues: string[] = []
            const kv: string[] = []
            const r2: string[] = []
            const all = Object.keys(env)
            for (let i = 0; i < all.length; i++) {
                if (!all[i]) {
                    continue
                }
                const obj = env[all[i]]
                if (!obj || typeof obj != 'object') {
                    continue
                }
                if (
                    (obj as Queue<string>) &&
                    typeof (obj as Queue<string>).send == 'function'
                ) {
                    queues.push(all[i])
                } else if (
                    (obj as KVNamespace) &&
                    typeof (obj as KVNamespace).getWithMetadata == 'function'
                ) {
                    kv.push(all[i])
                } else if (
                    (obj as R2Bucket) &&
                    typeof (obj as R2Bucket).createMultipartUpload == 'function'
                ) {
                    r2.push(all[i])
                }
            }

            const res = JSON.stringify({
                version,
                queues: queues && queues.length ? queues : undefined,
                kv: kv && kv.length ? kv : undefined,
                r2: r2 && r2.length ? r2 : undefined,
            })
            return new Response(res, {
                headers: {
                    'content-type': 'application/json',
                },
            })
        }
    )
    .post(
        '/publish/:queue',
        async (
            req: Request & RequestI,
            env: Environment
        ): Promise<Response> => {
            if (!req?.text || !req.params?.queue) {
                return new Response('Bad request', { status: 400 })
            }
            const queue = env[req.params.queue] as Queue<string>
            if (!queue || typeof queue.send != 'function') {
                return new Response(
                    `Not subscribed to queue '${req.params.queue}'`,
                    { status: 412 }
                )
            }

            const auth = await AuthorizeRequest(req, env)
            if (!auth) {
                return new Response('Unauthorized', { status: 401 })
            }

            let message = await req.text()
            await queue.send(message)
            return new Response('', { status: 201 })
        }
    )
    // Catch-all route to handle 404s
    .all('*', (): Response => {
        return new Response('Not found', { status: 404 })
    })

export default {
    fetch: router.handle,
}
