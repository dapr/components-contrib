#!/bin/sh

# Notice that while hashicorp supports multiple keys in a secret,
# our confirmance tests needs to go for the common demominator 
# which is a secret store that only has name/value semantic.
# Hence we setup secret containing a single key with the their
# same name.

set -eu

MAX_ATTEMPTS=30

for attempt in `seq $MAX_ATTEMPTS`; do
    # Test connectivity to vault server and create secrets to match
    # conformance tests / contents from tests/conformance/secrets.json
    if vault status &&
        vault secrets enable -path=customSecretsPath kv-v2 &&  # Enable this path with kv/version2 engine
        vault kv put customSecretsPath/dapr/secretUnderCustomPath the=trick was=the path=parameter &&
        vault kv get customSecretsPath/dapr/secretUnderCustomPath ;
    then
        echo ✅ secrets set;
        sleep 1;
        exit 0;
    else
        echo "⏰ vault not available, waiting... - attempt $attempt of $MAX_ATTEMPTS";
        sleep 1;
    fi
done;

echo ❌ Failed to set secrets;
exit 1
