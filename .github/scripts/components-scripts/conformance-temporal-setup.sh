#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-temporal.yml -p temporal up -d
