#!/bin/sh
$(dapr run --app-id=secretsapp --dapr-http-port 3500 --app-protocol grpc --components-path components/) &
DAPRD_PID=$!
sleep 5
curl --silent http://127.0.0.1:3500/v1.0/secrets/azurekeyvault/secondsecret
kill $DAPRD_PID
