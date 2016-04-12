#!/bin/bash
set -e

# Start secondary watcher locally and build it if necessary
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BASEDIR="$(dirname "${REALPATH}")/.."

CONFIG=${SODA_CONFIG:-/etc/soda2.conf} # TODO: Don't depend on soda2.conf.

cd "$BASEDIR"
JARFILE="$(ls -rt coordinator/target/scala-*/coordinator-assembly-*.jar 2>/dev/null | tail -n 1)"
if [ -z "$JARFILE" ] || find ./* -newer "$JARFILE" | egrep -q -v '(/target/)|(/bin/)'; then
    nice -n 19 sbt assembly
    JARFILE="$(ls -rt coordinator/target/scala-*/coordinator-assembly-*.jar 2>/dev/null | tail -n 1)"
fi

java -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -jar "$JARFILE" com.socrata.datacoordinator.secondary.SecondaryWatcher
