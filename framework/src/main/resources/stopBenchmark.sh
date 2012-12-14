#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh
. ${RADARGUN_HOME}/bin/environment.sh

CP=${RADARGUN_HOME}/lib/radargun-*.jar
JAVA="org.radargun.StopJmxRequest"
OBJ="Benchmark"

help_and_exit() {
echo "usage: $0 [hostname<:port>]..."
exit 0;
}

for slave in $@; do

if [[ "$slave" == *:* ]]; then
HOST="-hostname "`echo $slave | cut -d: -f1`
PORT="-port "`echo $slave | cut -d: -f2`
else
HOST="-hostname "$slave
PORT="-port "${JMX_SLAVES_PORT}
fi

CMD="java -cp ${CP} ${JAVA} ${HOST} ${PORT}"
echo $CMD
eval $CMD

done
exit 0