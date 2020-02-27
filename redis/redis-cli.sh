#!/bin/sh

[ -z ${1} ] && CONTAINER_NAME="redis_cli" || CONTAINER_NAME="redis_cli_${1}"

docker run --rm -it \
  --name "${CONTAINER_NAME}" \
  --network molecula_net \
  redis:alpine redis-cli -h redis_backend