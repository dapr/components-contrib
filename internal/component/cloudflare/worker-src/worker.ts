import { Router, type Request as RequestI } from 'itty-router'
import { importSPKI, jwtVerify } from 'jose'

import { version } from './package.json'

const router = Router()
    .get(
        '/.well-known/dapr/info',
        async (req: Request & RequestI, env: Environment): Promise<Response> => {
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

    async queue(batch: MessageBatch<string>, env: Environment) {
        for (let i = 0; i < batch.messages.length; i++) {
            console.log(`Received message ${JSON.stringify(batch.messages[i])}`)
        }
    },
}

const tokenHeaderMatch =
    /^(?:Bearer )?([A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+)/i

async function AuthorizeRequest(
    req: Request,
    env: Environment
): Promise<boolean> {
    // Ensure we have an Authorization header with a bearer JWT token
    const match = tokenHeaderMatch.exec(req.headers.get('authorization') || '')
    if (!match || !match[1]) {
        return false
    }

    // Validate the JWT
    const pk = await importSPKI(env.PUBLIC_KEY, 'EdDSA')
    try {
        await jwtVerify(match[1], pk, {
            issuer: 'dapr.io/cloudflare',
            audience: env.TOKEN_AUDIENCE,
        })
    } catch (err) {
        console.error('Failed to validate JWT: ' + err)
        return false
    }

    return true
}

type Environment = {
    PUBLIC_KEY: string
    TOKEN_AUDIENCE: string
    // Other values are assumed to be bindings: Queues, KV, R2
    readonly [x: string]: string | Queue<string> | KVNamespace | R2Bucket
}
