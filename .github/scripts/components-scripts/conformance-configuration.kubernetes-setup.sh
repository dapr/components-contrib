#!/bin/sh

set -e

# Create the ConfigMap used by the conformance test
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: dapr-conf-test
  namespace: default
data: {}
EOF

echo "NAMESPACE=default" >> $GITHUB_ENV
