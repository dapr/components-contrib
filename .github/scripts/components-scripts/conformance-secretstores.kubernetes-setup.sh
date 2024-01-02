#!/bin/sh

set -e

kubectl apply -f tests/config/kind-data.yaml
echo "NAMESPACE=default" >> $GITHUB_ENV
