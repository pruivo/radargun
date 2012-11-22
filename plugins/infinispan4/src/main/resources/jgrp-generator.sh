#!/bin/bash

WORKING_DIR=`cd $(dirname $0); cd ..; pwd`

DEST_FILE=${WORKING_DIR}/conf/jgroups/jgroups.xml

IP_MCAST="true"
INITIAL_HOST=`hostname -s`
TCP="false"

help() {
echo "usage: $0 <options>"
echo "  options:"
echo "    -sequencer                puts the SEQUENCER in protocol stack (Total Order Broadcast)"
echo ""
echo "    -toa                      puts the TOA in protocol stack (Total Order Anycast)"
echo ""
echo "    -no-ipmcast               sets the protocol stack for the case where IP Multicast does not exists"
echo ""
echo "    -gossip-host <address>    sets the initial host (used when you have -no-ipmcast)"
echo "                              Default: ${INITIAL_HOST}"
echo ""
echo "    -tcp                      sets the transport protocol as TCP"
echo "                              Default: uses UDP"
echo ""
echo "    -bundle                   forces the bundling protocol. In UDP it is enabled by default"
echo ""
echo "    -flow-control             forces the flow control protocol. In UDP it is enabled by default"
echo ""
echo "    -frag                     forces the fragmentation protocol. In UDP it is enabled by default"
echo ""
echo "    -compress <level>         sets the compression protocol. The <level>, between 1 and 9, sets the"
echo "                              compression level"
echo "                              Disabled by default"
echo ""
echo "    -h                        show this message"
}

while [ -n "$1" ]; do
case $1 in
  -h) help; exit 0;;
  -sequencer) SEQUENCER="true"; shift 1;;
  -toa) TOA="true"; shift 1;;
  -no-ipmcast) IP_MCAST="false"; shift 1;;
  -gossip-host) INITIAL_HOST=$2; shift 2;;
  -tcp) TCP="true"; shift 1;;
  -bundle) FORCE_BUNDLE="true"; shift 1;;
  -flow-control) FORCE_FLOW_CONTROL="true"; shift 1;;
  -frag) FORCE_FRAG="true"; shift 1;;
  -compress) COMPRESS=$2; shift 2;;
  *) echo "WARNING: unknown argument '$1'. It will be ignored" >&2; shift 1;;
  esac
done

echo "Writing configuration to ${DEST_FILE}"

echo "<config" > ${DEST_FILE}
echo "      xmlns=\"urn:org:jgroups\"" >> ${DEST_FILE}
echo "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" >> ${DEST_FILE}
echo "      xsi:schemaLocation=\"urn:org:jgroups http://www.jgroups.org/schema/JGroups-3.1.xsd\">" >> ${DEST_FILE}

if [ "${TCP}" == "true" ]; then
#TCP parameters!!
echo "   <TCP" >> ${DEST_FILE}
echo "         bind_port=\"7800\"" >> ${DEST_FILE}
echo "         recv_buf_size=\"\${tcp.recv_buf_size:20M}\"" >> ${DEST_FILE}
echo "         send_buf_size=\"\${tcp.send_buf_size:640K}\"" >> ${DEST_FILE}
echo "         use_send_queues=\"true\"" >> ${DEST_FILE}
echo "         sock_conn_timeout=\"300\"" >> ${DEST_FILE}
else
#UDP parameters!!
echo "   <UDP" >> ${DEST_FILE}
echo "         tos=\"8\"" >> ${DEST_FILE}
echo "         ucast_recv_buf_size=\"20M\"" >> ${DEST_FILE}
echo "         ucast_send_buf_size=\"640K\"" >> ${DEST_FILE}
echo "         ip_mcast=\"${IP_MCAST}\"" >> ${DEST_FILE}
echo "         ip_ttl=\"\${jgroups.udp.ip_ttl:8}\"" >> ${DEST_FILE}

#only if we have IP Multicast
if [ "${IP_MCAST}" == "true" ]; then
    echo "         mcast_recv_buf_size=\"25M\"" >> ${DEST_FILE}
    echo "         mcast_send_buf_size=\"640K\"" >> ${DEST_FILE}
    echo "         mcast_addr=\"\${jgroups.udp.mcast_addr:232.11.11.11}\"" >> ${DEST_FILE}
    echo "         mcast_port=\"\${jgroups.udp.mcast_port:45589}\"" >> ${DEST_FILE}
fi
fi

echo "         loopback=\"false\"" >> ${DEST_FILE}
echo "         discard_incompatible_packets=\"true\"" >> ${DEST_FILE}
if [ "${TCP}" == "false" -o "${FORCE_BUNDLE}" == "true" ]; then
echo "         max_bundle_size=\"64K\"" >> ${DEST_FILE}
echo "         max_bundle_timeout=\"2\"" >> ${DEST_FILE}
echo "         enable_bundling=\"true\"" >> ${DEST_FILE}
echo "         enable_unicast_bundling=\"true\"" >> ${DEST_FILE}
else
echo "         enable_bundling=\"false\"" >> ${DEST_FILE}
echo "         enable_unicast_bundling=\"false\"" >> ${DEST_FILE}
fi
echo "         enable_diagnostics=\"true\"" >> ${DEST_FILE}
echo "         thread_naming_pattern=\"cl\"" >> ${DEST_FILE}

echo "         thread_pool.enabled=\"true\"" >> ${DEST_FILE}
echo "         thread_pool.min_threads=\"8\"" >> ${DEST_FILE}
echo "         thread_pool.max_threads=\"64\"" >> ${DEST_FILE}
echo "         thread_pool.keep_alive_time=\"30000\"" >> ${DEST_FILE}
echo "         thread_pool.queue_enabled=\"true\"" >> ${DEST_FILE}
echo "         thread_pool.queue_max_size=\"10000\"" >> ${DEST_FILE}
echo "         thread_pool.rejection_policy=\"discard\"" >> ${DEST_FILE}

echo "         oob_thread_pool.enabled=\"true\"" >> ${DEST_FILE}
echo "         oob_thread_pool.min_threads=\"8\"" >> ${DEST_FILE}
echo "         oob_thread_pool.max_threads=\"64\"" >> ${DEST_FILE}
echo "         oob_thread_pool.keep_alive_time=\"30000\"" >> ${DEST_FILE}
echo "         oob_thread_pool.queue_enabled=\"true\"" >> ${DEST_FILE}
echo "         oob_thread_pool.queue_max_size=\"10000\"" >> ${DEST_FILE}
echo "         oob_thread_pool.rejection_policy=\"discard\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}

if [ "${IP_MCAST}" == "true" ]; then
   if [ "${TCP}" == "true" ]; then
#TPC discover
echo "   <MPING" >> ${DEST_FILE}
echo "         mcast_addr=\"\${jgroups.udp.mcast_addr:228.11.11.11}\"" >> ${DEST_FILE}
echo "         mcast_port=\"\${jgroups.udp.mcast_port:45589}\"" >> ${DEST_FILE}
echo "         ip_ttl=\"\${jgroups.udp.ip_ttl:8}\"" >> ${DEST_FILE}
else
#UDP discover
echo "   <PING" >> ${DEST_FILE}
fi
else
#we don't have IP Multicast
echo "   <TCPGOSSIP" >> ${DEST_FILE}
echo "         initial_hosts=\"\${jgroups.gossip_host:${INITIAL_HOST}}[12001]\"" >> ${DEST_FILE}
fi
#Discover common properties
echo "         num_initial_members=\"5\"" >> ${DEST_FILE}
echo "         break_on_coord_rsp=\"true\"" >> ${DEST_FILE}
#echo "         return_entire_cache=\"true\"" >> ${DEST_FILE}
echo "         stagger_timeout=\"3000\"" >> ${DEST_FILE}
echo "         timeout=\"15000\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}

echo "   <MERGE2" >> ${DEST_FILE}
echo "         max_interval=\"30000\"" >> ${DEST_FILE}
echo "         min_interval=\"10000\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}

echo "   <FD_SOCK/>" >> ${DEST_FILE}

echo "   <BARRIER/>" >> ${DEST_FILE}

echo "   <pbcast.NAKACK2" >> ${DEST_FILE}
if [ "${TCP}" == "false" -a "${IP_MCAST}" == "true" ]; then
echo "         use_mcast_xmit=\"true\"" >> ${DEST_FILE}
else
echo "         use_mcast_xmit=\"false\"" >> ${DEST_FILE}
fi
echo "         discard_delivered_msgs=\"true\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}

#TCP uses unicast2 and UDP uses unicast
if [ "${TCP}" == "true" ]; then
echo "   <UNICAST2" >> ${DEST_FILE}
echo "         max_stable_msgs=\"100\"" >> ${DEST_FILE}
echo "         xmit_interval=\"1000\"" >> ${DEST_FILE}
echo "         conn_expiry_timeout=\"0\"" >> ${DEST_FILE}
echo "         max_bytes=\"10M\"" >> ${DEST_FILE}
echo "         stable_interval=\"10000\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
else
echo "   <UNICAST" >> ${DEST_FILE}
echo "         max_retransmit_time=\"0\"" >> ${DEST_FILE}
echo "         conn_expiry_timeout=\"0\"" >> ${DEST_FILE}
echo "         xmit_interval=\"1000\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
fi

echo "   <pbcast.STABLE" >> ${DEST_FILE}
echo "         stability_delay=\"2000\"" >> ${DEST_FILE}
echo "         desired_avg_gossip=\"10000\"" >> ${DEST_FILE}
echo "         max_bytes=\"10M\"" >> ${DEST_FILE}
echo "         cap=\"0.001\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}

echo "   <pbcast.GMS" >> ${DEST_FILE}
echo "         print_local_addr=\"true\"" >> ${DEST_FILE}
echo "         join_timeout=\"3000\"" >> ${DEST_FILE}
echo "         max_bundling_time=\"500\"" >> ${DEST_FILE}
echo "         view_bundling=\"true\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}

#if sequencer
if [ "${SEQUENCER}" == "true" ]; then
echo "   <SEQUENCER/>" >> ${DEST_FILE}
fi

#if toa
if [ "${TOA}" == "true" ]; then
echo "   <tom.TOA/>" >> ${DEST_FILE}
fi

if [ "${TCP}" == "false" -o "${FORCE_FLOW_CONTROL}" == "true" ]; then
echo "   <UFC" >> ${DEST_FILE}
echo "         max_credits=\"4M\"" >> ${DEST_FILE}
echo "         min_threshold=\"0.4\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
echo "   <MFC" >> ${DEST_FILE}
echo "         max_credits=\"4M\"" >> ${DEST_FILE}
echo "         min_threshold=\"0.4\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
fi

if [ "${TCP}" == "false" -o "${FORCE_FRAG}" == "true" ]; then
echo "   <FRAG2" >> ${DEST_FILE}
echo "         frag_size=\"60K\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
fi

echo "   <pbcast.STATE_TRANSFER/>" >> ${DEST_FILE}

if [ "${COMPRESS}" != "" ]; then
echo "   <COMPRESS" >> ${DEST_FILE}
echo "         compression_level=\"${COMPRESS}\"" >> ${DEST_FILE}
echo "         min_size=\"10K\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
fi

echo "</config>" >> ${DEST_FILE}

echo "Finished!"