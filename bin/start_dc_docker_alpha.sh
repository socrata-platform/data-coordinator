#!/usr/bin/env bash

set -e

image="$1"
if [ -z "$image" ]; then
  echo "ERR: must pass the name of the docker image to run as the first argument to this script!"
  exit 1
fi

local_config_dir="$(dirname "$(realpath "$0")")/../configs"

docker run \
  -e SERVER_CONFIG=/etc/configs/application-alpha.conf \
  -v "$local_config_dir":/etc/configs \
  -p 6020:6020 \
  "$image"
