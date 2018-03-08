#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

$BINDIR/start_dc.sh "$BINDIR/../configs/application-bravo.conf"
