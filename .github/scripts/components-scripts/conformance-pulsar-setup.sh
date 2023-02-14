#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-pulsar.yml -p pulsar up -d
