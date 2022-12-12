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

import { importSPKI, jwtVerify } from 'jose'

import type { Environment } from '$lib/environment'

const tokenHeaderMatch =
    /^(?:Bearer )?([A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+)/i

export async function AuthorizeRequest(
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
            algorithms: ['EdDSA'],
            // Allow 5 mins of clock skew
            clockTolerance: 300,
        })
    } catch (err) {
        console.error('Failed to validate JWT: ' + err)
        return false
    }

    return true
}
