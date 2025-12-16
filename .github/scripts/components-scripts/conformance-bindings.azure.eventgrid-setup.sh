#!/bin/sh

set -e

echo "Downloading ngrok..."
wget https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-linux-amd64.tgz
tar -xzf ngrok-v3-stable-linux-amd64.tgz
./ngrok authtoken ${AzureEventGridNgrokToken}

echo "Starting ngrok tunnel..."
./ngrok http --log=stdout --log-level=debug --host-header=localhost 9000 > /tmp/ngrok.log 2>&1 &
NGROK_PID=$!

echo "Waiting for ngrok to start..."
sleep 10

# Ensure ngrok is still running
if ! kill -0 $NGROK_PID 2>/dev/null; then
    echo "Ngrok process died. Log output:"
    cat /tmp/ngrok.log
    exit 1
fi

echo "Getting tunnel URL from ngrok API..."
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s http://localhost:4040/api/tunnels > /tmp/ngrok_api.json 2>/dev/null; then
        # Extract the public URL from the API response
        NGROK_ENDPOINT=$(cat /tmp/ngrok_api.json | grep -o '"public_url":"[^"]*"' | head -1 | cut -d'"' -f4)
        
        if [ -n "$NGROK_ENDPOINT" ] && [ "$NGROK_ENDPOINT" != "null" ]; then
            echo "Successfully got tunnel URL from API: $NGROK_ENDPOINT"
            break
        fi
    fi
    
    echo "Waiting for ngrok API to be ready... (attempt $((RETRY_COUNT + 1))/$MAX_RETRIES)"
    sleep 2
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

echo "Ngrok endpoint: ${NGROK_ENDPOINT}"
echo "AzureEventGridSubscriberEndpoint=${NGROK_ENDPOINT}/api/events" >> $GITHUB_ENV
echo "Ngrok log:"
cat /tmp/ngrok.log

# Schedule trigger to kill ngrok
bash -c "sleep 600 && pkill ngrok" &
