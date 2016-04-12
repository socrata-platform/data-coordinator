#!/bin/bash
set -e

# Start secondary watcher locally and build it if necessary
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")

CONFIG="${SODA_CONFIG:-/etc/soda2.conf}" # TODO: Don't depend on soda2.conf.
JARFILE=$("$REALPATH"/build.sh)

java -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -jar "$JARFILE" com.socrata.datacoordinator.secondary.SecondaryWatcher
