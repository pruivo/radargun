package org.radargun.cachewrappers;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
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
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

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
   Transport transport;

   private BucketsKeysTreeSet keys;

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
         transport = cacheManager.getTransport();

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

   @Override
   public boolean isCoordinator(){
      return this.cacheManager.isCoordinator();
   }

   @Override
   public boolean isKeyLocal(String bucket, String key) {
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

   @Override
   public boolean canExecuteReadOnlyTransactions() {
      return !isPassiveReplication() || (transport != null && !transport.isCoordinator());
   }

   @Override
   public boolean canExecuteWriteTransactions() {
      return !isPassiveReplication() || (transport != null && transport.isCoordinator());
   }

   @Override
   public void resetAdditionalStats() {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      String domain = cacheManager.getGlobalConfiguration().getJmxDomain();
      for(ObjectName name : mBeanServer.queryNames(null, null)) {
         if(name.getDomain().equals(domain)) {
            tryResetStats(name, mBeanServer);
         }
      }
   }

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

   private boolean isPassiveReplication() {
      try {
         Method method = Configuration.class.getMethod("isPassiveReplication");
         return (Boolean) method.invoke(cache.getConfiguration());
      } catch (Exception e) {
         log.debug("isPassiveReplication method not found or can't be invoked. Assuming passive replication in use");
      }
      return false;
   }

   //================================================= JMX STATS ====================================================

   private void tryResetStats(ObjectName component, MBeanServer mBeanServer) {
      Object[] emptyArgs = new Object[0];
      String[] emptySig = new String[0];
      try {
         mBeanServer.invoke(component, "resetStatistics", emptyArgs, emptySig);
         return;
      } catch (Exception e) {
         log.debug("resetStatistics not found in " + component);
      }
      try {
         mBeanServer.invoke(component, "resetStats", emptyArgs, emptySig);
         return;
      } catch (Exception e) {
         log.debug("resetStats not found in " + component);
      }
      try {
         mBeanServer.invoke(component, "reset", emptyArgs, emptySig);
         return;
      } catch (Exception e) {
         log.debug("reset not found in " + component);
      }
      log.warn("No stats were reset for component " + component);
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
            toValidator  = new ObjectName(baseName + "TotalOrderManager");
            if (!mBeanServer.isRegistered(toValidator)) {
               log.info("Not collecting statistics from Total Order component. It is not registered");
               return;
            }
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
