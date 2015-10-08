#!/bin/bash
# Run data coordinator migrations
# run_migrations.sh [migrate/undo/redo] [numchanges]
BASEDIR=$(dirname "$0")/..
CONFIG=${SODA_CONFIG:-$BASEDIR/../docs/onramp/services/soda2.conf}

JARS=( $BASEDIR/coordinator/target/scala-2.10/coordinator-assembly-*.jar )
# shellcheck disable=SC2012
JARFILE=$(ls -t "${JARS[@]}" | head -n 1)

if [ ! -e "$JARFILE" ]; then
    cd "$BASEDIR" && sbt assembly
    JARS=( $BASEDIR/coordinator/target/scala-2.10/coordinator-assembly-*.jar )
    # shellcheck disable=SC2012
    JARFILE=$(ls -t "${JARS[@]}" | head -n 1)
fi

COMMAND=${1:-migrate}
echo Running datacoordinator.primary.MigrateSchema "$COMMAND" "$2"...
ARGS=( $COMMAND $2 )
java -Dconfig.file="$CONFIG" -jar "$JARFILE" com.socrata.datacoordinator.primary.MigrateSchema "${ARGS[@]}"
