#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-memcached.yml -p memcached up -d
