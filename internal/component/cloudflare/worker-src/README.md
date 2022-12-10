# Dapr connector for Cloudflare Workers

## Develop locally

```sh
npm install
npm run start
```

## Info endpoint

```sh
curl "http://localhost:8787/.well-known/dapr/info"
```

## Publish a message

```sh
curl -X POST -d 'hello world' "http://localhost:8787/publish/daprdemo"
```
