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

import { Router, IRequest } from 'itty-router'

import { Environment } from '$lib/environment'
import { AuthorizeRequest } from '$lib/jwt-auth'

import { version } from './package.json'

const router = Router()
    // Handle the info endpoint
    .get(
        '/.well-known/dapr/info',
        async (req: IRequest, env: Environment): Promise<Response> => {
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
                if (!obj || typeof obj != 'object' || !obj.constructor) {
                    continue
                }
                switch (obj.constructor.name) {
                    case 'KvNamespace':
                    case 'KVNamespace':
                        kv.push(all[i])
                        break
                    case 'WorkerQueue':
                    case 'Queue':
                        queues.push(all[i])
                        break
                    case 'R2Bucket':
                        // Note that we currently don't support R2 yet
                        r2.push(all[i])
                        break
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

    // Retrieve a value from KV
    .get(
        '/kv/:namespace/:key',
        async (req: IRequest, env: Environment): Promise<Response> => {
            const { namespace, key, errorRes } = await setupKVRequest(req, env)
            if (errorRes) {
                return errorRes
            }

            const val = await namespace!.get(key!, 'stream')
            if (!val) {
                return new Response('', { status: 404 })
            }

            return new Response(val, { status: 200 })
        }
    )

    // Store a value in KV
    .post(
        '/kv/:namespace/:key',
        async (req: IRequest, env: Environment): Promise<Response> => {
            const { namespace, key, errorRes } = await setupKVRequest(req, env)
            if (errorRes) {
                return errorRes
            }

            let expirationTtl: number | undefined = undefined
            const reqUrl = new URL(req.url)
            const ttlParam = parseInt(reqUrl.searchParams.get('ttl') || '', 10)
            if (ttlParam > 0) {
                expirationTtl = ttlParam
            }
            await namespace!.put(key!, req.body!, { expirationTtl })

            return new Response('', { status: 201 })
        }
    )

    // Delete a value from KV
    .delete(
        '/kv/:namespace/:key',
        async (req: IRequest, env: Environment): Promise<Response> => {
            const { namespace, key, errorRes } = await setupKVRequest(req, env)
            if (errorRes) {
                return errorRes
            }

            await namespace!.delete(key!)

            return new Response('', { status: 204 })
        }
    )

    // Publish a message in a queue
    .post(
        '/queues/:queue',
        async (req: IRequest, env: Environment): Promise<Response> => {
            const { queue, errorRes } = await setupQueueRequest(req, env)
            if (errorRes) {
                return errorRes
            }

            let message = await req.text()
            await queue!.send(message)
            return new Response('', { status: 201 })
        }
    )

    // Catch-all route to handle 404s
    .all('*', (): Response => {
        return new Response('Not found', { status: 404 })
    })

// Performs the init setps for a KV request. Returns a Response object in case of error.
async function setupKVRequest(
    req: IRequest,
    env: Environment
): Promise<{
    namespace?: KVNamespace<string>
    key?: string
    errorRes?: Response
}> {
    if (!req?.text || !req.params?.namespace || !req.params?.key) {
        return { errorRes: new Response('Bad request', { status: 400 }) }
    }
    const namespace = env[req.params.namespace] as KVNamespace<string>
    if (
        typeof namespace != 'object' ||
        !['KVNamespace', 'KvNamespace'].includes(namespace?.constructor?.name)
    ) {
        return {
            errorRes: new Response(
                `Worker is not bound to KV '${req.params.kv}'`,
                { status: 412 }
            ),
        }
    }

    const auth = await AuthorizeRequest(req, env)
    if (!auth) {
        return { errorRes: new Response('Unauthorized', { status: 401 }) }
    }

    return { namespace, key: req.params.key }
}

// Performs the init setps for a Queue request. Returns a Response object in case of error.
async function setupQueueRequest(
    req: IRequest,
    env: Environment
): Promise<{ queue?: Queue<string>; errorRes?: Response }> {
    if (!req?.text || !req.params?.queue) {
        return { errorRes: new Response('Bad request', { status: 400 }) }
    }
    const queue = env[req.params.queue] as Queue<string>
    if (
        typeof queue != 'object' ||
        !['WorkerQueue', 'Queue'].includes(queue?.constructor?.name)
    ) {
        return {
            errorRes: new Response(
                `Worker is not bound to queue '${req.params.queue}'`,
                { status: 412 }
            ),
        }
    }

    const auth = await AuthorizeRequest(req, env)
    if (!auth) {
        return { errorRes: new Response('Unauthorized', { status: 401 }) }
    }

    return { queue }
}

export default {
    fetch: router.handle,
}
