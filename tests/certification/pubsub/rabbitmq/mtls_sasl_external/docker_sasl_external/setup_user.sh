#!/bin/sh
sleep 10
echo SETUPSTART
rabbitmqctl add_user buildkitsandbox ""
rabbitmqctl set_user_tags buildkitsandbox administrator
rabbitmqctl set_permissions -p / buildkitsandbox ".*" ".*" ".*"
rabbitmqctl set_permissions -p / guest ".*" ".*" ".*"
echo SETUPSTOP