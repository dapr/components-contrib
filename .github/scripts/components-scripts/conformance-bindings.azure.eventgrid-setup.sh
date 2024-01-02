#!/bin/sh

set -e

# Start ngrok
wget https://bin.equinox.io/c/4VmDzA7iaHb/ngrok-stable-linux-amd64.zip
unzip -qq ngrok-stable-linux-amd64.zip
./ngrok authtoken ${AzureEventGridNgrokToken}
./ngrok http -log=stdout --log-level debug -host-header=localhost 9000 > /tmp/ngrok.log &
sleep 10

NGROK_ENDPOINT=`cat /tmp/ngrok.log |  grep -Eom1 'https://.*' | sed 's/\s.*//'`
echo "Ngrok endpoint: ${NGROK_ENDPOINT}"
echo "AzureEventGridSubscriberEndpoint=${NGROK_ENDPOINT}/api/events" >> $GITHUB_ENV
cat /tmp/ngrok.log

# Schedule trigger to kill ngrok
bash -c "sleep 600 && pkill ngrok" &
