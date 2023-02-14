#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-redis7.yml -p redis up -d
