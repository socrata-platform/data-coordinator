#!/bin/bash
set -e

# Start data coordinator locally and build it if necessary
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

CONFIG=$1
if [[ -z $CONFIG ]]; then
  CONFIG="configs/application.conf"
fi

JARFILE=$("$BINDIR"/build.sh "$@")

"$BINDIR"/run_migrations.sh "$CONFIG" migrate

java -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -jar "$JARFILE" com.socrata.datacoordinator.service.Main
