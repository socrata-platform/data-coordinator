#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")
# Run data coordinator migrations
# run_migrations.sh [migrate/undo/redo] [numchanges]
CONFIG="${SODA_CONFIG:-"$BINDIR"/../configs/application.conf}" # TODO: Don't depend on soda2.conf.

COMMAND=${1:-migrate}
echo Running datacoordinator.primary.MigrateSchema "$COMMAND" "$2"...
ARGS=( $COMMAND $2 )

JARFILE=$("$BINDIR"/build.sh "$@")

java -Dconfig.file="$CONFIG" -jar "$JARFILE" com.socrata.datacoordinator.primary.MigrateSchema "${ARGS[@]}"
