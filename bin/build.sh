#!/bin/bash
set -e

# Start data coordinator locally and build it if necessary
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BASEDIR="$(dirname "${REALPATH}")/.."

cd "$BASEDIR"
JARFILE="coordinator/target/coordinator-assembly.jar"
SRC_PATHS=($(find .  -maxdepth 2 -name 'src' -o -name '*.sbt' -o -name '*.scala'))
if [ ! -f "$JARFILE" ] || find "${SRC_PATHS[@]}" -newer "$JARFILE" | egrep -q -v '(/target/)|(/bin/)'; then
    if [ "$1" == '--fast' ]; then
        echo 'Assembly is out of date.  Fast start is enabled so skipping rebuild anyway...' >&2
    else
        nice -n 19 sbt coordinator/assembly >&2
        touch "$JARFILE"
    fi
fi

python -c "import os; print(os.path.realpath('$JARFILE'))"
