#!/bin/sh

set +e

# Stop ngrok
echo "GET ngrok tunnels:"
curl http://localhost:4040/api/tunnels
echo "GET ngrok http requests:"
curl http://localhost:4040/api/requests/http
pkill ngrok
cat /tmp/ngrok.log
