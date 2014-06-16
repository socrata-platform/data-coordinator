#!/bin/bash
# Run data coordinator migrations
# run_migrations.sh [migrate/undo/redo] [numchanges]
BASEDIR=$(dirname $0)/..
CONFIG=${SODA_CONFIG:-$BASEDIR/../onramp/services/soda2.conf}
JARFILE=$BASEDIR/coordinator/target/scala-2.10/coordinator-assembly-*.jar
if [ ! -e $JARFILE ]; then
  cd $BASEDIR && sbt assembly
fi
COMMAND=${1:-migrate}
echo Running datacoordinator.primary.MigrateSchema $COMMAND $2...
java -Dconfig.file=$CONFIG -jar $JARFILE com.socrata.datacoordinator.primary.MigrateSchema $COMMAND $2
