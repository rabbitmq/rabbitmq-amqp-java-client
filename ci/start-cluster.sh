#!/usr/bin/env bash

export RABBITMQ_IMAGE=${RABBITMQ_IMAGE:-pivotalrabbitmq/rabbitmq:v4.0.x}

wait_for_message() {
  while ! docker logs "$1" | grep -q "$2";
  do
      sleep 2
      echo "Waiting 2 seconds for $1 to start..."
  done
}

docker compose --file ci/cluster/docker-compose.yml down
docker compose --file ci/cluster/docker-compose.yml up --detach

wait_for_message rabbitmq0 "completed with"

docker exec rabbitmq0 rabbitmqctl await_online_nodes 3

docker exec rabbitmq0 rabbitmqctl enable_feature_flag khepri_db
docker exec rabbitmq1 rabbitmqctl enable_feature_flag khepri_db
docker exec rabbitmq2 rabbitmqctl enable_feature_flag khepri_db

docker exec rabbitmq0 rabbitmqctl cluster_status