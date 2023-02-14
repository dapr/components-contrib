#!/bin/sh

docker-compose -f .github/infrastructure/docker-compose-hashicorp-vault.yml -p vault up -d
