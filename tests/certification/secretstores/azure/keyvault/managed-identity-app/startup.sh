#!/bin/sh
$(dapr run --app-id=secretsapp --dapr-http-port 3500 --app-protocol grpc --components-path components/) &
sleep 5
curl http://127.0.0.1:3500/v1.0/secrets/azurekeyvault/bulk