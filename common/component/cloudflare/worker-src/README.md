# Dapr connector for Cloudflare Workers

This folder contains the source code for the Worker that is used by Dapr components to interact with Cloudflare services such as KV and Queues.

## Version

If you make changes to the Worker, please change the version number in the `package.json` file. The code is versioned by date in the YYYYMMDD format.

## Build code

The built Worker resides in `../workers/code`. You can build it with:

```sh
npm ci
npm run build
```

> Important: do not deploy this worker (e.g. with `npx wrangler deploy`), as it should not use the config in the `wrangler.toml` file!

## Develop locally

Note that when running locally, authorization is not required and all Authorization headers are ignored. Settings for development are read from the `wrangler.toml` file, which is not used by Dapr.

### Create a Queue

The default configuration in `wrangler.toml` (used for development only) includes a binding to a Queue called `daprdemo`. If you don't have it already, make sure to create it with:

```sh
npx wrangler queues create daprdemo
```

### Create a KV namespace

To test with KV, you need to first create a namespace with Wrangler, for example:

```sh
npx wrangler kv:namespace create daprkv
npx wrangler kv:namespace create daprkv --preview
```

The output contains something like:

```text
Add the following to your configuration file in your kv_namespaces array:
{ binding = "daprkv", id = "......" }
Add the following to your configuration file in your kv_namespaces array:
{ binding = "daprkv", preview_id = "......" }
```

Make sure to add the values of `id` and `preview_id` above to the `wrangler.toml` file.

### Start the application locally

Start the application locally, using Wrangler

```sh
npm ci
npm run start
```

### Info endpoint

```sh
curl "http://localhost:8787/.well-known/dapr/info"
```

### Using KV

Store a value:

```sh
# Format is /kv/<KV namespace>/<key>
curl -X POST -d 'Hello world!' "http://localhost:8787/kv/daprkv/mykey"
# Success: 201 (Created), empty body
```

Retrieve a value:

```sh
# Format is /kv/<KV namespace>/<key>
curl "http://localhost:8787/kv/daprkv/mykey"
# Success: 200 (OK), value in body
# No key: 404 (Not found), empty body
```

Delete a value:

```sh
# Format is /kv/<KV namespace>/<key>
curl -X DELETE "http://localhost:8787/kv/daprkv/mykey"
# Success: 204 (No content), empty body
```

### Using Queues

Publish a message:

```sh
# Format is /queues/<queue name>
curl -X POST -d 'orders.42' "http://localhost:8787/queues/daprdemo"
# Success: 201 (Accepted), empty body
```

## Disabling authentication for testing

By default, all requests to the Worker need to be authenticated using a JWT. For testing purposes, you can disable that by setting an environmental variable `SKIP_AUTH` with value `true` in your Worker. This is not recommended for production.
