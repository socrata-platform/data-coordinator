#!/bin/bash
set -e

# Run data coordinator migrations
# run_migrations.sh [migrate/undo/redo] [numchanges]
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BASEDIR="$(dirname "${REALPATH}")/.."

CONFIG="${SODA_CONFIG:-/etc/soda2.conf}" # TODO: Don't depend on soda2.conf.

cd "$BASEDIR"
JARFILE="$(ls -rt coordinator/target/scala-*/coordinator-assembly-*.jar 2>/dev/null | tail -n 1)"
if [ -z "$JARFILE" ] || find ./* -newer "$JARFILE" | egrep -q -v '(/target/)|(/bin/)'; then
    nice -n 19 sbt assembly
    JARFILE="$(ls -rt coordinator/target/scala-*/coordinator-assembly-*.jar 2>/dev/null | tail -n 1)"
fi

COMMAND=${1:-migrate}
echo Running datacoordinator.primary.MigrateSchema "$COMMAND" "$2"...
ARGS=( $COMMAND $2 )
java -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -jar "$JARFILE" com.socrata.datacoordinator.primary.MigrateSchema "${ARGS[@]}"
