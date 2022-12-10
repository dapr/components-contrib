import { Router, type Request } from 'itty-router'

import { version } from './package.json'

const router = Router()
    .get(
        '/.well-known/dapr/info',
        (request: Request, env: Environment): Response => {
            // Filter all bindings by type
            const queues: string[] = []
            const kv: string[] = []
            const r2: string[] = []
            const all = Object.keys(env)
            for (let i = 0; i < all.length; i++) {
                if (!all[i]) {continue }
                const obj = env[all[i]]
                if (!obj || typeof obj != 'object') { continue }
                if ((obj as Queue<string>) && typeof (obj as Queue<string>).send == 'function') {
                    queues.push(all[i])
                } else if ((obj as KVNamespace) && typeof (obj as KVNamespace).getWithMetadata == 'function') {
                    kv.push(all[i])
                } else if ((obj as R2Bucket) && typeof (obj as R2Bucket).createMultipartUpload == 'function') {
                    r2.push(all[i])
                }
            }

            const res = JSON.stringify({
                version,
                queues: (queues && queues.length) ? queues: undefined,
                kv: (kv && kv.length) ? kv: undefined,
                r2: (r2 && r2.length) ? r2: undefined,
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
        async (request: Request, env: Environment): Promise<Response> => {
            if (!request?.text || !request.params?.queue) {
                return new Response('Bad request', { status: 400 })
            }
            const queue = env[request.params.queue] as Queue<string>
            if (!queue || typeof queue.send != 'function') {
                return new Response(
                    `Not subscribed to queue '${request.params.queue}'`,
                    { status: 412 }
                )
            }
            let message = await request.text()
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

    async queue(batch: MessageBatch<string>, env: Environment) {
        for (let i = 0; i < batch.messages.length; i++) {
            console.log(`Received message ${JSON.stringify(batch.messages[i])}`)
        }
    },
}

type Environment = {
    readonly [x in string]: Queue<string> | KVNamespace | R2Bucket
}
