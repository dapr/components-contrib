#!/bin/sh

set +e

# Wait for the creation of the DB by the test to propagate to ARM, otherwise deletion succeeds as no-op.
# The wait should be under 30s, but is capped at 1m as flakiness here results in an accumulation of expensive DB instances over time.
# Also note that the deletion call only blocks until the request is process, do not rely on it for mutex on the same DB,
# deletion may be ongoing in sequential runs.
sleep 1m
az sql db delete --resource-group "$AzureResourceGroupName" --server "$AzureSqlServerName" -n "$AzureSqlServerDbName" --yes
