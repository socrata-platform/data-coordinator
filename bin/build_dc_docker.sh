#!/usr/bin/env bash

set -e

sbt coordinator/assembly
JARFILE="$(ls -t coordinator/target/scala-2.12/coordinator-assembly*.jar | head -1)"
cp "$JARFILE" "coordinator/docker/coordinator-assembly.jar"
docker build --pull=true -t data-coordinator coordinator/docker
