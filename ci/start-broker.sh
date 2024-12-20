#!/usr/bin/env bash

RABBITMQ_IMAGE=${RABBITMQ_IMAGE:-rabbitmq:4.0}

wait_for_message() {
  while ! docker logs "$1" | grep -q "$2";
  do
      sleep 2
      echo "Waiting 2 seconds for $1 to start..."
  done
}

make -C "${PWD}"/tls-gen/basic

rm -rf rabbitmq-configuration
mkdir -p rabbitmq-configuration/tls
cp -R "${PWD}"/tls-gen/basic/result/* rabbitmq-configuration/tls
chmod o+r rabbitmq-configuration/tls/*
chmod g+r rabbitmq-configuration/tls/*

echo "[rabbitmq_auth_mechanism_ssl,rabbitmq_auth_backend_oauth2]." >> rabbitmq-configuration/enabled_plugins

echo "loopback_users = none

listeners.ssl.default = 5671

deprecated_features.permit.amqp_address_v1 = false

ssl_options.cacertfile = /etc/rabbitmq/tls/ca_certificate.pem
ssl_options.certfile   = /etc/rabbitmq/tls/server_$(hostname)_certificate.pem
ssl_options.keyfile    = /etc/rabbitmq/tls/server_$(hostname)_key.pem
ssl_options.verify     = verify_peer
ssl_options.fail_if_no_peer_cert = false
ssl_options.depth = 1

auth_mechanisms.1 = PLAIN
auth_mechanisms.2 = ANONYMOUS
auth_mechanisms.3 = EXTERNAL

auth_backends.1 = internal
auth_backends.2 = rabbit_auth_backend_oauth2" >> rabbitmq-configuration/rabbitmq.conf

echo "[
  {rabbitmq_auth_backend_oauth2, [{key_config,
         [{signing_keys,
              #{<<\"token-key\">> =>
                    {map,
                        #{<<\"alg\">> => <<\"HS256\">>,
                          <<\"k\">> => <<\"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH\">>,
                          <<\"kid\">> => <<\"token-key\">>,
                          <<\"kty\">> => <<\"oct\">>,
                          <<\"use\">> => <<\"sig\">>,
                          <<\"value\">> => <<\"token-key\">>}}}}]},
     {resource_server_id,<<\"rabbitmq\">>}]}
]." >> rabbitmq-configuration/advanced.config

echo "Running RabbitMQ ${RABBITMQ_IMAGE}"

docker rm -f rabbitmq 2>/dev/null || echo "rabbitmq was not running"
docker run -d --name rabbitmq \
    --network host \
    -v "${PWD}"/rabbitmq-configuration:/etc/rabbitmq \
    "${RABBITMQ_IMAGE}"

wait_for_message rabbitmq "completed with"

docker exec rabbitmq rabbitmqctl enable_feature_flag --opt-in khepri_db
docker exec rabbitmq rabbitmq-diagnostics erlang_version
docker exec rabbitmq rabbitmqctl version
