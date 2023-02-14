#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-kubemq.yml -p kubemq up -d
