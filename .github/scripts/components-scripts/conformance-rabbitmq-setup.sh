#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-rabbitmq.yml -p rabbitmq up -d
