#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-hazelcast.yml -p hazelcast up -d
