#!/bin/bash
set -e

# Bootstrap data coordinator database including running migrations
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

INSTANCE=$1
if [[ -z $INSTANCE ]]; then
  echo "Usage: bootstrap_dc_db.sh instance [config]"
  exit 1
fi

DATABASE="datacoordinator_$INSTANCE"
echo "Creating data coordinator database $DATABASE with owner blist..."
createdb --owner=blist --encoding=utf-8 "$DATABASE"

CONFIG=$2
if [[ -z $CONFIG ]]; then
  CONFIG="configs/application.conf"
fi

"$BINDIR"/run_migrations.sh "$CONFIG" migrate