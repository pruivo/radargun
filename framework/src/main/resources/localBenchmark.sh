#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh

help_and_exit() {
  wrappedecho "Usage: "
  wrappedecho '  $ localBenchmark.sh -n <value>'    
  wrappedecho ""
  wrappedecho "   -n       The number of local slaves to start"
  wrappedecho ""
  exit 0
}
### read in any command-line params
while ! [ -z $1 ]
do
  case "$1" in
    "-n")
      N_SLAVES=$2
      shift
      ;;
    *)      
        echo "Warning: unknown argument ${1}" 
        help_and_exit      
      ;;
  esac
  shift
done

if [ -z "$N_SLAVES" ] ; then
   help_and_exit    
fi

####### first start the master
. ${RADARGUN_HOME}/bin/master.sh -s $N_SLAVES -i $N_SLAVES
PID_OF_MASTER_PROCESS=$RADARGUN_MASTER_PID

#### Sleep for a few seconds so master can open its port
sleep 5s
####### then start the rest of the nodes
CMD="${RADARGUN_HOME}/bin/slave.sh"

port=10000;
i=0;

while [ $i -lt $N_SLAVES ]; do
  port=`echo "${port}+${i}" | bc`
  TOEXEC="${CMD} -l $i -p $i -jmx ${port}"
  echo "$TOEXEC"
  eval $TOEXEC
  i=`echo "${i}+1" | bc`
done

echo "Slaves started"
exit 0