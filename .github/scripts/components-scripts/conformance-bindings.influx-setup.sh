#!/bin/sh

set -e

export INFLUX_TOKEN=$(openssl rand -base64 32)
echo "INFLUX_TOKEN=$INFLUX_TOKEN" >> $GITHUB_ENV
docker compose -f .github/infrastructure/docker-compose-influxdb.yml -p influxdb up -d
