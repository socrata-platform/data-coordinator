#!/bin/bash
set -e

# Run data coordinator migrations
# run_migrations.sh [migrate/undo/redo] [numchanges]
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

CONFIG="${SODA_CONFIG:-/etc/soda2.conf}" # TODO: Don't depend on soda2.conf.
JARFILE=$("$BINDIR"/build.sh)

COMMAND=${1:-migrate}
echo Running datacoordinator.primary.MigrateSchema "$COMMAND" "$2"...
ARGS=( $COMMAND $2 )
java -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -jar "$JARFILE" com.socrata.datacoordinator.primary.MigrateSchema "${ARGS[@]}"
