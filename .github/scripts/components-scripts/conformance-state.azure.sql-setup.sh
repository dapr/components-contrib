#!/bin/sh

set -e

# Use UUID with `-` stripped out for DB names to prevent collisions between workflows
AzureSqlServerDbName=$(cat /proc/sys/kernel/random/uuid | sed -E 's/-//g')
echo "AzureSqlServerDbName=$AzureSqlServerDbName" >> $GITHUB_ENV
