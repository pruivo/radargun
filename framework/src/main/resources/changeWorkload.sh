#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh
. ${RADARGUN_HOME}/bin/environment.sh

CP=${RADARGUN_HOME}/lib/radargun-*.jar
JAVA="org.radargun.jmx.JmxRemoteOperation"
OP="highContention"
OBJ="TpccBenchmark"

help_and_exit() {
echo "usage: $0 [-high] [-low] [-read]"
exit 0;
}

while [ -n "$1" ]; do
case $1 in
  -high) OP="highContention"; shift 1;;
  -low) OP="lowContention"; shift 1;;
  -read) OP="lowContentionAndRead"; shift 1;;
  -h) help_and_exit;;
  -*) echo "Unknown option $1" shift 1;;
  *) echo "Unknown parameter $1" shift 1;;
esac
done

for slave in `cat ${RADARGUN_HOME}/slaves`; do

if [[ "$slave" == *:* ]]; then
HOST=`echo $slave | cut -d: -f1`
PORT=`echo $slave | cut -d: -f2`
else
HOST=$slave
PORT=${JMX_SLAVES_PORT}
fi

CMD="java -cp ${CP} ${JAVA} ${OBJ} ${OP} ${HOST} ${PORT}"
echo $CMD
eval $CMD

done
exit 0