#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-mosquitto.yml -p mosquitto up -d
