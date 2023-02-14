#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-vernemq.yml -p vernemq up -d
