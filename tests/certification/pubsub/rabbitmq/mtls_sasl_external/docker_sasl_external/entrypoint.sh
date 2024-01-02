#!/bin/sh
# docker entrypoint script for rabbitmq
./setup_user.sh &
rabbitmq-server $@