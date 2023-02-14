#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-redisjson.yml -p redis up -d
