#!/usr/bin/env bash

docker rm -f toxiproxy 2>/dev/null || echo "toxiproxy was not running"
docker run -d --name toxiproxy \
    --network host \
    ghcr.io/shopify/toxiproxy

wait_for_message rabbitmq "completed with"

docker exec rabbitmq rabbitmq-diagnostics erlang_version
docker exec rabbitmq rabbitmqctl version
