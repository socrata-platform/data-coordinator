#!/usr/bin/env bash

set -e

sbt coordinator/assembly
JARFILE="coordinator/target/coordinator-assembly.jar"
cp "$JARFILE" "coordinator/docker/coordinator-assembly.jar"
docker build --pull=true -t data-coordinator coordinator/docker
