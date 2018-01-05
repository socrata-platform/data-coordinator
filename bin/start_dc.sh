#!/bin/bash
set -e

# Start data coordinator locally and build it if necessary
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

CONFIG="${SODA_CONFIG:-"$BINDIR"/../configs/application.conf}"
JARFILE=$("$BINDIR"/build.sh "$@")

"$BINDIR"/run_migrations.sh

java -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -jar "$JARFILE" com.socrata.datacoordinator.service.Main
