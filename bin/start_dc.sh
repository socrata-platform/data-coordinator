#!/bin/bash
# Start data coordinator locally and build it if necessary
BASEDIR=$(dirname $0)/..
CONFIG=${SODA_CONFIG:-$BASEDIR/../docs/onramp/services/soda2.conf}
JARFILE=$BASEDIR/coordinator/target/scala-2.10/coordinator-assembly-*.jar
if [ ! -e $JARFILE ]; then
  cd $BASEDIR && sbt assembly
fi
java -Dconfig.file=$CONFIG -jar $JARFILE com.socrata.datacoordinator.service.Main &
