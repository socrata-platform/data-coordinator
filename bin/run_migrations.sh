#!/bin/bash
set -e

# Run data coordinator migrations
# run_migrations.sh [migrate/undo/redo] [numchanges]
CONFIG="${SODA_CONFIG:-/etc/soda2.conf}" # TODO: Don't depend on soda2.conf.

COMMAND=${1:-migrate}
echo Running datacoordinator.primary.MigrateSchema "$COMMAND" "$2"...
ARGS=( $COMMAND $2 )
sbt -Dconfig.file="$CONFIG" "coordinator/run-main com.socrata.datacoordinator.primary.MigrateSchema ${ARGS[@]}"
