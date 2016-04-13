#!/bin/bash
set -e

# Start data coordinator locally and build it if necessary
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BASEDIR="$(dirname "${REALPATH}")/.."

cd "$BASEDIR"
JARFILE="$(ls -rt coordinator/target/scala-*/coordinator-assembly-*.jar 2>/dev/null | tail -n 1)"
if [ -z "$JARFILE" ] || find ./* -newer "$JARFILE" | egrep -q -v '(/target/)|(/bin/)'; then
    nice -n 19 sbt assembly >&2
    JARFILE="$(ls -rt coordinator/target/scala-*/coordinator-assembly-*.jar 2>/dev/null | tail -n 1)"
    touch "$JARFILE"
fi

echo "$JARFILE"
