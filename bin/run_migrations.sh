#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")
# Run data coordinator migrations
# run_migrations.sh [config] [migrate/undo/redo] [numchanges]

CONFIG="configs/application.conf"
COMMANDS="migrate"
if [[ $1 == migrate || $1 == undo || $1 == redo ]]; then
  COMMANDS=( $1 $2 )
elif [[ ! -z $1 ]]; then
  CONFIG=$1
  if [[ ! -z $2 ]]; then
    COMMANDS=( $2 $3 )
  fi
fi

echo Running datacoordinator.primary.MigrateSchema "$COMMANDS" with "$CONFIG"...

JARFILE=$("$BINDIR"/build.sh "$@")

java -Dconfig.file="$CONFIG" -jar "$JARFILE" com.socrata.datacoordinator.primary.MigrateSchema "$COMMANDS"
