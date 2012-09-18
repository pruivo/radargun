#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh
. ${RADARGUN_HOME}/bin/environment.sh

CP=${RADARGUN_HOME}/lib/radargun-*.jar
JAVA="org.radargun.WorkloadJmxRequest"
OBJ="-jmxComponent BenchmarkStage"
PORT="-port "${JMX_SLAVES_PORT}
OPTIONS="";

help_and_exit() {
CMD="java -cp ${CP} ${JAVA} -h"
echo $CMD
eval $CMD
exit 0;
}

while [ -n "$1" ]; do
case $1 in
  -jmx-component) OBJ="-jmx-component $2"; shift 2;;
  -port) PORT="-port $2"; shift 2;;
  -h) help_and_exit;;
  *) OPTIONS=${OPTIONS}" "$1; shift 1;;  
esac
done

CMD="java -cp ${CP} ${JAVA} ${OBJ} ${OPTIONS} ${PORT}"
echo $CMD
eval $CMD

exit 0