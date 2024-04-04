#!/usr/bin/env bash

LOCAL_SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

RABBITMQ_IMAGE=${RABBITMQ_IMAGE:-pivotalrabbitmq/rabbitmq:amqp-address-otp-max-bazel}

wait_for_message() {
  while ! docker logs "$1" | grep -q "$2";
  do
      sleep 5
      echo "Waiting 5 seconds for $1 to start..."
  done
}

mkdir rabbitmq-configuration
echo "loopback_users = none" >> rabbitmq-configuration/rabbitmq.conf

echo "Running RabbitMQ ${RABBITMQ_IMAGE}"

docker rm -f rabbitmq 2>/dev/null || echo "rabbitmq was not running"
docker run -d --name rabbitmq \
    --network host \
    -v "${PWD}"/rabbitmq-configuration:/etc/rabbitmq \
    "${RABBITMQ_IMAGE}"

wait_for_message rabbitmq "completed with"

docker exec rabbitmq rabbitmq-diagnostics erlang_version
docker exec rabbitmq rabbitmqctl version
