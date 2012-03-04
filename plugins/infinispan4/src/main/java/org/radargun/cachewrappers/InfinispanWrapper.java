package org.radargun.cachewrappers;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.utils.Utils;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.TransactionManager;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.radargun.utils.Utils.mBeanAttributes2String;

public class InfinispanWrapper implements CacheWrapper {

    private static final String GET_ATTRIBUTE_ERROR = "Exception while obtaining the attribute [%s] from [%s]";

    private static Log log = LogFactory.getLog(InfinispanWrapper.class);
    DefaultCacheManager cacheManager;
    Cache<Object, Object> cache;
    TransactionManager tm;
    DistributionManager dm;
    boolean started = false;
    String config;


    public void setUp(String config, boolean isLocal, int nodeIndex) throws Exception {
        this.config = config;


        if (!started) {
            cacheManager = new DefaultCacheManager(config);
            // use a named cache, based on the 'default'
            cacheManager.defineConfiguration("x", new Configuration());
            cache = cacheManager.getCache("x");
            tm=cache.getAdvancedCache().getTransactionManager();
            dm=cache.getAdvancedCache().getDistributionManager();

            started = true;
        }

        log.info("Loading JGroups form: " + org.jgroups.Version.class.getProtectionDomain().getCodeSource().getLocation());
        log.info("JGroups version: " + org.jgroups.Version.printDescription());

        // should we be blocking until all rehashing, etc. has finished?
        long gracePeriod = MINUTES.toMillis(15);
        long giveup = System.currentTimeMillis() + gracePeriod;
        if (cache.getConfiguration().getCacheMode().isDistributed()) {
            while (!cache.getAdvancedCache().getDistributionManager().isJoinComplete() && System.currentTimeMillis() < giveup)
                Thread.sleep(200);
        }

        if (cache.getConfiguration().getCacheMode().isDistributed() && !cache.getAdvancedCache().getDistributionManager().isJoinComplete())
            throw new RuntimeException("Caches haven't discovered and joined the cluster even after " + Utils.prettyPrintTime(gracePeriod));
    }

    public void tearDown() throws Exception {
        List<Address> addressList = cacheManager.getMembers();
        if (started) {
            cacheManager.stop();
            log.trace("Stopped, previous view is " + addressList);
            started = false;
        }
    }

    public void put(String bucket, Object key, Object value) throws Exception {

        cache.put(key, value);

    }

    public void printNodes(Object key){
        List<Address> list=dm.locate(key);
        log.info("printNodes for key "+key);
        for(Address a: list){
            log.info(""+a);
        }
    }

    public Object get(String bucket, Object key) throws Exception {
        return cache.get(key);
    }

    public void empty() throws Exception {
        log.info("Cache size before clear: " + cache.size());
        cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
        log.info("Cache size after clear: " + cache.size());
    }

    public int getNumMembers() {
        ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
        if (componentRegistry.getStatus().startingUp()) {
            log.trace("We're in the process of starting up.");
        }
        if (cacheManager.getMembers() != null) {
            log.trace("Members are: " + cacheManager.getMembers());
        }
        return cacheManager.getMembers() == null ? 0 : cacheManager.getMembers().size();
    }

    public String getInfo() {
        String clusterSizeStr = "";
        RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
        if (rpcManager != null && rpcManager.getTransport() != null) {
            clusterSizeStr = "cluster size = " + rpcManager.getTransport().getMembers().size();
        }
        return cache.getVersion() + ", " + clusterSizeStr + ", " + config + ", Size of the cache is: " + cache.size();
    }

    public Object getReplicatedData(String bucket, String key) throws Exception {
        return get(bucket, key);
    }

    public Object startTransaction() {

        if (tm==null) return null ;

        try {
            tm.begin();
            return tm.getTransaction();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void endTransaction(boolean successful)throws RuntimeException{

        if (tm == null){
            return;
        }
        try {
            if (successful)
                tm.commit();
            else
                tm.rollback();
        }

        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    * Method to retrieve information about the replicas policy
     */

    public boolean isPassiveReplication(){
        return this.cache.getAdvancedCache().getConfiguration().isPassiveReplication();
    }

    public boolean isPrimary(){

        return this.cacheManager.isCoordinator();
    }

    public void switch_to_PC(){
        this.cache.getConfiguration().setReplicasPolicy(Configuration.ReplicasPolicyMode.PC);
    }

    public void switch_to_PR(){
        this.cache.getConfiguration().setReplicasPolicy(Configuration.ReplicasPolicyMode.PASSIVE_REPLICATION);
    }

    public Cache getCache(){
        return this.cache;
    }

    public String getCacheMode() {
        return cache.getConfiguration().getCacheModeString();
    }

    public String getNodeName(){
        return getCache().getCacheManager().getAddress().toString();
    }

    public boolean isFullyReplicated(){
        return (this.cache.getConfiguration().getCacheMode().isReplicated()) ||
                (this.cache.getConfiguration().getCacheMode().isDistributed() &&
                        this.cache.getConfiguration().getNumOwners() >=
                                ((this.cacheManager.getMembers() == null) ? 0 : this.cacheManager.getMembers().size()));
    }

    public boolean isLocal(Object key, int slice){

        boolean result = false;

        int myId=this.dm.getSelfID();
        List<Address> list = this.dm.locate(key);

        int firstId = this.dm.getAddressID(list.get(0));

        if(myId == firstId){

            result = true;
        }

        /*
         if(this.dm.getLocality(key).isLocal()){
             int numSlices = this.dm.getConsistentHash().getCaches().size();

             if((slice % numSlices) != numSlices-1){
                 int myId=this.dm.getSelfID();
                 Iterator<Address> itr=this.dm.locate(key).iterator();
                 int minId=this.dm.getAddressID(itr.next());

                 int currentId;
                 while(itr.hasNext()){
                     currentId = this.dm.getAddressID(itr.next());
                     if(currentId<minId){
                         minId=currentId;
                     }

                 }

                 result=(minId == myId);
             }
             else{
                 int myId=this.dm.getSelfID();
                 Iterator<Address> itr=this.dm.locate(key).iterator();
                 int maxId=this.dm.getAddressID(itr.next());

                 int currentId;
                 while(itr.hasNext()){
                     currentId = this.dm.getAddressID(itr.next());
                     if(currentId>maxId){
                         maxId=currentId;
                     }

                 }

                 result=(maxId == myId);
             }

         }
         */

        return result;


    }

    @Override
    public int getCacheSize() {
        return cache.size();
    }

    //================================================= JMX STATS ====================================================
    @Override
    public Map<String, String> getAdditionalStats() {
        Map<String, String> results = new HashMap<String, String>();
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        String cacheComponentString = getCacheComponentBaseString(mBeanServer);

        if(cacheComponentString != null) {
            getStatsFromTransactionInterceptor(cacheComponentString, mBeanServer, results);
            getStatsFromDistributionManager(cacheComponentString, mBeanServer, results);
            getStatsFromRPCManager(cacheComponentString, mBeanServer, results);
            getStatsFromDistributionInterceptor(cacheComponentString, mBeanServer, results);
            saveStatsFromStreamLibStatistics(cacheComponentString, mBeanServer);
            getStatsFromTotalOrderValidator(cacheComponentString, mBeanServer, results);
        } else {
            log.info("Not collecting additional stats. Infinspan MBeans not found");
        }
        return results;
    }

    private String getCacheComponentBaseString(MBeanServer mBeanServer) {
        String domain = cacheManager.getGlobalConfiguration().getJmxDomain();
        for(ObjectName name : mBeanServer.queryNames(null, null)) {
            if(name.getDomain().equals(domain)) {

                if("Cache".equals(name.getKeyProperty("type"))) {
                    String cacheName = name.getKeyProperty("name");
                    String cacheManagerName = name.getKeyProperty("manager");
                    return new StringBuilder(domain)
                            .append(":type=Cache,name=")
                            .append(cacheName.startsWith("\"") ? cacheName :
                                    ObjectName.quote(cacheName))
                            .append(",manager=").append(cacheManagerName.startsWith("\"") ? cacheManagerName :
                                    ObjectName.quote(cacheManagerName))
                            .append(",component=").toString();
                }
            }
        }
        return null;
    }

    private void saveStatsFromStreamLibStatistics(String baseName, MBeanServer mBeanServer) {
        try {
            ObjectName streamLibStats = new ObjectName(baseName + "StreamLibStatistics");

            if (!mBeanServer.isRegistered(streamLibStats)) {
                log.info("Not collecting statistics from Stream Lib component. It is no registered");
                return;
            }

            String filePath = "top-keys-" + cache.getAdvancedCache().getRpcManager().getAddress();

            log.info("Collecting statistics from Stream Lib component [" + streamLibStats + "] and save them in " +
                    filePath);
            log.debug("Attributes available are " +
                    mBeanAttributes2String(mBeanServer.getMBeanInfo(streamLibStats).getAttributes()));

            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));

            bufferedWriter.write("RemoteTopGets=" + getMapAttribute(mBeanServer, streamLibStats,"RemoteTopGets")
                    .toString());
            bufferedWriter.newLine();
            bufferedWriter.write("LocalTopGets=" + getMapAttribute(mBeanServer, streamLibStats,"LocalTopGets")
                    .toString());
            bufferedWriter.newLine();
            bufferedWriter.write("RemoteTopPuts=" + getMapAttribute(mBeanServer, streamLibStats,"RemoteTopPuts")
                    .toString());
            bufferedWriter.newLine();
            bufferedWriter.write("LocalTopPuts=" + getMapAttribute(mBeanServer, streamLibStats,"LocalTopPuts")
                    .toString());
            bufferedWriter.newLine();
            bufferedWriter.flush();
            bufferedWriter.close();

        } catch (Exception e) {
            log.warn("Unable to collect stats from Stream Lib Statistic component");
        }
    }

    private void getStatsFromTotalOrderValidator(String baseName, MBeanServer mBeanServer, Map<String, String> results) {
        try {
            ObjectName toValidator = new ObjectName(baseName + "TotalOrderValidator");

            if (!mBeanServer.isRegistered(toValidator)) {
                log.info("Not collecting statistics from Total Order component. It is not registered");
                return;
            }

            log.info("Collecting statistics from Total Order component [" + toValidator + "]");
            log.debug("Attributes available are " +
                    mBeanAttributes2String(mBeanServer.getMBeanInfo(toValidator).getAttributes()));

            double avgWaitingQueue = getDoubleAttribute(mBeanServer, toValidator, "averageWaitingTimeInQueue");
            double avgValidationDur = getDoubleAttribute(mBeanServer, toValidator, "averageValidationDuration");
            double avgInitDur = getDoubleAttribute(mBeanServer, toValidator, "averageInitializationDuration");

            results.put("AVG_WAITING_TIME_IN_QUEUE(msec)", String.valueOf(avgWaitingQueue));
            results.put("AVG_VALIDATION_DURATION(msec)", String.valueOf(avgValidationDur));
            results.put("AVG_INIT_DURATION(msec)", String.valueOf(avgInitDur));
        } catch (Exception e) {
            log.warn("Unable to collect stats from Total Order Validator component");
        }
    }

    private void getStatsFromTransactionInterceptor(String baseName, MBeanServer mBeanServer, Map<String, String> results) {
        try {
            ObjectName txInter = new ObjectName(baseName + "Transactions");

            if (!mBeanServer.isRegistered(txInter)) {
                log.info("Not collecting statistics from Transaction Interceptor component. It is not registered");
                return;
            }

            log.info("Collecting statistics from Transaction Interceptor component [" + txInter + "]");
            log.debug("Attributes available are " +
                    mBeanAttributes2String(mBeanServer.getMBeanInfo(txInter).getAttributes()));

            long successRemotePrepareDuration = getLongAttribute(mBeanServer, txInter, "SuccessPrepareTime");
            long numberOfSuccessRemotePrepare = getLongAttribute(mBeanServer, txInter, "NrSuccessPrepare");
            if(numberOfSuccessRemotePrepare > 0) {
                results.put("SUCCESSFUL_REMOTE_PREPARE_PHASE_TIME(nsec)",
                        String.valueOf(successRemotePrepareDuration/numberOfSuccessRemotePrepare));
            } else {
                results.put("SUCCESSFUL_REMOTE_PREPARE_PHASE_TIME(nsec)", String.valueOf(0));
            }

            long failedRemotePrepareDuration = getLongAttribute(mBeanServer, txInter, "FailedPrepareTime");
            long numberOfFailedRemotePrepare = getLongAttribute(mBeanServer, txInter, "NrFailedPrepare");
            if(numberOfFailedRemotePrepare > 0) {
                results.put("FAILED_REMOTE_PREPARE_PHASE_TIME(nsec)",
                        String.valueOf(failedRemotePrepareDuration / numberOfFailedRemotePrepare));
            } else {
                results.put("FAILED_REMOTE_PREPARE_PHASE_TIME(nsec)", String.valueOf(0));
            }

            long remoteCommitDuration = getLongAttribute(mBeanServer, txInter, "RemoteCommitTime");
            long numberOfRemoteCommits = getLongAttribute(mBeanServer, txInter, "NrRemoteCommit");
            if(numberOfRemoteCommits > 0) {
                results.put("REMOTE_COMMIT_PHASE_TIME(nsec)",
                        String.valueOf(remoteCommitDuration / numberOfRemoteCommits));
            } else {
                results.put("REMOTE_COMMIT_PHASE_TIME(nsec)", String.valueOf(0));
            }

            long localCommitDuration = getLongAttribute(mBeanServer, txInter, "LocalCommitTime");
            long numberOfLocalCommits = getLongAttribute(mBeanServer, txInter, "NrLocalCommit");
            if(numberOfLocalCommits > 0) {
                results.put("LOCAL_COMMIT_PHASE_TIME(nsec)",
                        String.valueOf(localCommitDuration / numberOfLocalCommits));
            } else {
                results.put("LOCAL_COMMIT_PHASE_TIME(nsec)", String.valueOf(0));
            }

            long RWLocksAcquisitionTime = getLongAttribute(mBeanServer, txInter, "RWLocksAcquisitionTime");
            long NrRWLocksAcquisitions = getLongAttribute(mBeanServer, txInter, "NrRWLocksAcquisitions");
            if(NrRWLocksAcquisitions > 0) {
                results.put("RW_LOCKS_ACQUISITION_TIME(nsec)",
                        String.valueOf(RWLocksAcquisitionTime / NrRWLocksAcquisitions));
            } else {
                results.put("RW_LOCKS_ACQUISITION_TIME(nsec)", String.valueOf(0));
            }

            long remoteRollbackDuration = getLongAttribute(mBeanServer, txInter, "RollbackTime");
            long numberOfRemoteRollback = getLongAttribute(mBeanServer, txInter, "NrRollback");
            if(numberOfRemoteRollback > 0) {
                results.put("REMOTE_ROLLBACK_PHASE_TIME(nsec)",
                        String.valueOf(remoteRollbackDuration / numberOfRemoteRollback));
            } else {
                results.put("REMOTE_ROLLBACK_PHASE_TIME(nsec)", String.valueOf(0));
            }

            long numberOfRollbacksDueToUnableAcquireLocks = getLongAttribute(mBeanServer, txInter,
                    "RollbacksDueToUnableAcquireLock");
            results.put("ROLLBACKS_FOR_FAILED_AQUIRE_LOCK", String.valueOf(numberOfRollbacksDueToUnableAcquireLocks));

            long numberOfRollbacksDueToDeadlocks = getLongAttribute(mBeanServer, txInter, "RollbacksDueToDeadLock");
            results.put("ROLLBACKS_FOR_DEADLOCKS", String.valueOf(numberOfRollbacksDueToDeadlocks));

            long numberOfRollbacksDueToValidation = getLongAttribute(mBeanServer, txInter, "RollbacksDueToValidation");
            results.put("ROLLBACKS_FOR_FAILED_VALIDATION", String.valueOf(numberOfRollbacksDueToValidation));

            long getKeyDuration = getLongAttribute(mBeanServer, txInter, "ReadTime");
            long numberOfGetKey = getLongAttribute(mBeanServer, txInter, "NrReadOp");
            if(numberOfGetKey > 0) {
                results.put("GET_DURATION(nsec)", String.valueOf(getKeyDuration / numberOfGetKey));
            } else {
                results.put("GET_DURATION(nsec)", String.valueOf(0));
            }

            long getLocalKeyDuration = getLongAttribute(mBeanServer, txInter, "LocalReadTime");
            long numberOfLocalGetKey = getLongAttribute(mBeanServer, txInter, "NrLocalReadOp");
            if(numberOfLocalGetKey > 0) {
                results.put("LOCAL_GET_DURATION(nsec)", String.valueOf(getLocalKeyDuration / numberOfLocalGetKey));
            } else {
                results.put("LOCAL_GET_DURATION(nsec)", String.valueOf(0));
            }

            long remoteGetKeyDuration = getLongAttribute(mBeanServer, txInter, "RemoteReadTime");
            long numberOfRemoteGetKey = getLongAttribute(mBeanServer, txInter, "NrRemoteReadOp");
            if(numberOfRemoteGetKey > 0) {
                results.put("FROM_REMOTE_GET_DURATION(nsec)", String.valueOf(remoteGetKeyDuration / numberOfRemoteGetKey));
            } else {
                results.put("FROM_REMOTE_GET_DURATION(nsec)", String.valueOf(0));

            }

            results.put("LOCAL_GET", String.valueOf(numberOfLocalGetKey));
            results.put("REMOTE_GET", String.valueOf(numberOfGetKey - numberOfLocalGetKey));

        } catch (Exception e) {
            log.warn("Unable to collect stats from Transaction Interceptor component");
        }
    }

    private void getStatsFromDistributionManager(String baseName, MBeanServer mBeanServer, Map<String, String> results) {
        try {
            ObjectName distMan = new ObjectName(baseName + "DistributionManager");

            if (!mBeanServer.isRegistered(distMan)) {
                log.info("Not collecting statistics from Distribution Manager component. It is not registered");
                return;
            }

            log.info("Collecting statistics from Distribution Manager component [" + distMan + "]");
            log.debug("Attributes available are " +
                    mBeanAttributes2String(mBeanServer.getMBeanInfo(distMan).getAttributes()));

            long rttOfRemoteGetKey = getLongAttribute(mBeanServer, distMan, "RoundTripTimeClusteredGetKey");
            long numberOfRemoteGetKeyII = getLongAttribute(mBeanServer, distMan, "NrClusteredGet");
            if(numberOfRemoteGetKeyII > 0) {
                results.put("REMOTE_GET_RESPONSE_LATENCY(nsec)",
                        String.valueOf(rttOfRemoteGetKey / numberOfRemoteGetKeyII));
            } else {
                results.put("REMOTE_GET_RESPONSE_LATENCY(nsec)", String.valueOf(0));
            }
        } catch (Exception e) {
            log.warn("Unable to collect stats from Distribution Manager component");
        }
    }

    private void getStatsFromRPCManager(String baseName, MBeanServer mBeanServer, Map<String, String> results) {
        try {
            ObjectName rpcMan = new ObjectName(baseName + "RpcManager");

            if (!mBeanServer.isRegistered(rpcMan)) {
                log.info("Not collecting statistics from RPC Manager component. It is not registered");
                return;
            }

            log.info("Collecting statistics from RPC Manager component [" + rpcMan + "]");
            log.debug("Attributes available are " +
                    mBeanAttributes2String(mBeanServer.getMBeanInfo(rpcMan).getAttributes()));

            long prepareCommandBytes = getLongAttribute(mBeanServer, rpcMan, "PrepareCommandBytes");
            long numberOfPrepareCommand = getLongAttribute(mBeanServer, rpcMan, "NrPrepareCommand");
            if(numberOfPrepareCommand > 0) {
                results.put("SIZE_PREPARE_COMMAND(bytes)", String.valueOf(prepareCommandBytes / numberOfPrepareCommand));
            } else {
                results.put("SIZE_PREPARE_COMMAND(bytes)", String.valueOf(0));
            }

            long commitCommandBytes = getLongAttribute(mBeanServer, rpcMan, "CommitCommandBytes");
            long numberOfCommitCommand = getLongAttribute(mBeanServer, rpcMan, "NrCommitCommand");
            if(numberOfCommitCommand > 0) {
                results.put("SIZE_COMMIT_COMMAND(bytes)", String.valueOf(commitCommandBytes / numberOfCommitCommand));
            } else {
                results.put("SIZE_COMMIT_COMMAND(bytes)", String.valueOf(0));
            }

            long clusteredGetKeyCommandBytes = getLongAttribute(mBeanServer, rpcMan, "ClusteredGetKeyCommandBytes");
            long numberOfClusteredGetKeyCommand = getLongAttribute(mBeanServer, rpcMan, "NrClusteredGetKeyCommand");
            if(numberOfClusteredGetKeyCommand > 0) {
                results.put("SIZE_CLUSTERED_GET_KEY_COMMAND(bytes)",
                        String.valueOf(clusteredGetKeyCommandBytes / numberOfClusteredGetKeyCommand));
            } else {
                results.put("SIZE_CLUSTERED_GET_KEY_COMMAND(bytes)", String.valueOf(0));
            }

        } catch (Exception e) {
            log.warn("Unable to collect stats from RPC Manager component");
        }
    }

    private void getStatsFromDistributionInterceptor(String baseName, MBeanServer mBeanServer, Map<String, String> results) {
        try {
            ObjectName distInterceptor = new ObjectName(baseName + "DistributionInterceptor");

            if (!mBeanServer.isRegistered(distInterceptor)) {
                log.info("Not collecting statistics from Distribution Interceptor component. It is not registered");
                return;
            }

            log.info("Collecting statistics from Distribution Interceptor component [" + distInterceptor + "]");
            log.debug("Attributes available are " +
                    mBeanAttributes2String(mBeanServer.getMBeanInfo(distInterceptor).getAttributes()));

            long totalNumOfInvolvedNodesPerPrepare = getLongAttribute(mBeanServer, distInterceptor,
                    "TotalNumOfInvolvedNodesPerPrepare");
            long totalPrepareSent = getLongAttribute(mBeanServer, distInterceptor, "TotalPrepareSent");
            if(totalPrepareSent > 0) {
                results.put("AVG_NUM_INVOLVED_NODES_PER_PREPARE",
                        String.valueOf((totalNumOfInvolvedNodesPerPrepare * 1.0) / totalPrepareSent));
            } else {
                results.put("AVG_NUM_INVOLVED_NODES_PER_PREPARE", String.valueOf(0));
            }
        } catch (Exception e) {
            log.warn("Unable to collect stats from Distribution Interceptor component");
        }
    }

    private Long getLongAttribute(MBeanServer mBeanServer, ObjectName component, String attr) {
        try {
            return (Long)mBeanServer.getAttribute(component, attr);
        } catch (Exception e) {
            log.debug(String.format(GET_ATTRIBUTE_ERROR, attr, component), e);
        }
        return -1L;
    }

    private Double getDoubleAttribute(MBeanServer mBeanServer, ObjectName component, String attr) {
        try {
            return (Double)mBeanServer.getAttribute(component, attr);
        } catch (Exception e) {
            log.debug(String.format(GET_ATTRIBUTE_ERROR, attr, component), e);
        }
        return -1D;
    }

    private Map<Object, Object> getMapAttribute(MBeanServer mBeanServer, ObjectName component, String attr) {
        try {
            return (Map<Object, Object>)mBeanServer.getAttribute(component, attr);
        } catch (Exception e) {
            log.debug(String.format(GET_ATTRIBUTE_ERROR, attr, component), e);
        }
        return Collections.emptyMap();
    }

}
