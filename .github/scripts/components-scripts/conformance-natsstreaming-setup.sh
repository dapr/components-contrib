#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-natsstreaming.yml -p natsstreaming up -d
