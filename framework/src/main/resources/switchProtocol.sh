#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh
. ${RADARGUN_HOME}/bin/environment.sh

CP=${RADARGUN_HOME}/lib/radargun-*.jar
JAVA="org.radargun.SwitchJmxRequest"
PROTOCOLS="PB,2PC,TO,PB,TO,2PC"

help_and_exit() {
echo "usage: $0 <slave>"
echo "   slave: hostname or hostname:port"
exit 0;
}

if [ -n "$1" ]; then
slave=$1;
fi

if [[ "$slave" == *:* ]]; then
HOST=`echo $slave | cut -d: -f1`
PORT=`echo $slave | cut -d: -f2`
else
HOST=$slave
fi

fullHostname=$HOST" "$PORT

for protocol in $PROTOCOLS; do
echo "sleeping 2min"
sleep 2m
echo "Switch to "$protocol
CMD="java -cp ${CP} ${JAVA} ${protocol} ${fullHostname}"
echo $CMD
eval $CMD
done

CMD="java -cp ${CP} ${JAVA} -print-stats something ${fullHostname}"
echo $CMD
eval $CMD

exit 0