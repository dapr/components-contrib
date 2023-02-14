#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-emqx.yml -p emqx up -d
