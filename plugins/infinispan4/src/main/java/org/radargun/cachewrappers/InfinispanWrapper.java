package org.radargun.cachewrappers;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.utils.BucketsKeysTreeSet;
import org.radargun.utils.Utils;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.TransactionManager;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.radargun.utils.Utils.mBeanAttributes2String;

public class InfinispanWrapper implements CacheWrapper {
    private static final String GET_ATTRIBUTE_ERROR = "Exception while obtaining the attribute [%s] from [%s]";

    private static Log log = LogFactory.getLog(InfinispanWrapper.class);
    DefaultCacheManager cacheManager;
    Cache<Object, Object> cache;
    TransactionManager tm;
    boolean started = false;
    String config;

    private BucketsKeysTreeSet keys;

    private static enum Stats {
        FAILURES_DUE_TO_TIMEOUT,
        FAILURES_DUE_TO_DEADLOCK,
        FAILURES_DUE_TO_WRITE_SKEW
    }

    private final AtomicLong[] stats = new AtomicLong[Stats.values().length];
    {
        for(int i = 0; i < stats.length; i++) {
            stats[i] = new AtomicLong(0);
        }
    }


    public void setUp(String config, boolean isLocal, int nodeIndex) throws Exception {
        this.config = config;


        if (!started) {
            cacheManager = new DefaultCacheManager(config);
//          if (!isLocal) {
//             GlobalConfiguration configuration = cacheManager.getGlobalConfiguration();
//             configuration.setTransportNodeName(String.valueOf(nodeIndex));
//          }
            // use a named cache, based on the 'default'
            cacheManager.defineConfiguration("x", new Configuration());
            cache = cacheManager.getCache("x");
            tm=cache.getAdvancedCache().getTransactionManager();

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
        try {
            cache.put(key, value);
        } catch(TimeoutException e) {
            stats[Stats.FAILURES_DUE_TO_TIMEOUT.ordinal()].incrementAndGet();
            throw e;
        } catch (DeadlockDetectedException e) {
            stats[Stats.FAILURES_DUE_TO_DEADLOCK.ordinal()].incrementAndGet();
            throw e;
        } catch (CacheException e) {
            if(e.getMessage().startsWith("Detected write skew")) {
                stats[Stats.FAILURES_DUE_TO_WRITE_SKEW.ordinal()].incrementAndGet();
            }
            throw e;
        }
    }

    public Object get(String bucket, Object key) throws Exception {
        return cache.get(key);
    }

    public void empty() throws Exception {
        log.info("Cache size before clear: " + cache.size());
        cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
        log.info("Cache size after clear: " + cache.size());
        resetStats();
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
        if (tm == null) return null;

        try {
            tm.begin();
            return tm.getTransaction();
        } catch (Exception e) {
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void resetStats() {
        for (AtomicLong al : stats) {
            al.set(0);
        }
    }

    @Override
    public boolean isCoordinator(){
        return this.cacheManager.isCoordinator();
    }

    @Override
    public boolean isKeyLocal(String key) {
        DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
        return dm == null || dm.isLocal(key);
    }

    @Override
    public void saveKeysStressed(BucketsKeysTreeSet keys) {
        BucketsKeysTreeSet bucketsKeysTreeSet = new BucketsKeysTreeSet();

        if (keys != null) {
            Iterator<Map.Entry<String, SortedSet<String>>> it = keys.getEntrySet().iterator();
            if (it.hasNext()) {
                Map.Entry<String, SortedSet<String>> entry = it.next();
                bucketsKeysTreeSet.addKeySet("ISPN_BUCKET", entry.getValue());
            }
        }

        this.keys = bucketsKeysTreeSet;
    }

    @Override
    public BucketsKeysTreeSet getStressedKeys() {
        return keys != null ? keys : new BucketsKeysTreeSet();
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
