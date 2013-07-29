package org.radargun.cachewrappers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.radargun.CacheWrapper;
import org.radargun.cachewrappers.parser.StatisticComponent;
import org.radargun.cachewrappers.parser.StatsParser;
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
   private static final Log log = LogFactory.getLog(InfinispanWrapper.class);
   private final List<StatisticComponent> statisticComponents = StatsParser.parse("all-stats.xml");
   DefaultCacheManager cacheManager;
   Cache<Object, Object> cache;
   TransactionManager tm;
   boolean started = false;
   String config;
   Transport transport;
   Method isPassiveReplicationMethod = null;
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
         //cacheManager.defineConfiguration("x", new Configuration());
         cache = cacheManager.getCache("x");
         tm = cache.getAdvancedCache().getTransactionManager();
         transport = cache.getAdvancedCache().getRpcManager().getTransport();
         try {
            isPassiveReplicationMethod = Configuration.class.getMethod("isPassiveReplication");
         } catch (Exception e) {
            //just ignore
            isPassiveReplicationMethod = null;
         }
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

   public void endTransaction(boolean successful) throws RuntimeException {
      if (tm == null) {
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
   public boolean isCoordinator() {
      return this.cacheManager.isCoordinator();
   }

   @Override
   public boolean isKeyLocal(String bucket, String key) {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      return dm == null || dm.getLocality(key).isLocal();
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
      for (ObjectName name : mBeanServer.queryNames(null, null)) {
         if (name.getDomain().equals(domain)) {
            tryResetStats(name, mBeanServer);
         }
      }
   }

   @Override
   public Map<String, String> getAdditionalStats() {
      Map<String, String> results = new HashMap<String, String>();
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      String cacheComponentString = getCacheComponentBaseString(mBeanServer);

      if (cacheComponentString != null) {
         saveStatsFromStreamLibStatistics(cacheComponentString, mBeanServer);

         for (StatisticComponent statisticComponent : statisticComponents) {
            getStatsFrom(cacheComponentString, mBeanServer, results, statisticComponent);
         }
      } else {
         log.info("Not collecting additional stats. Infinispan MBeans not found");
      }
      return results;
   }

   @Override
   public void dumpDataContainer(String filePath) {
      //no-op
   }

   @Override
   public void dumpCommitLog(String filePath) {
      //no-op
   }

   private boolean isPassiveReplication() {
      try {
         return isPassiveReplicationMethod != null && (Boolean) isPassiveReplicationMethod.invoke(cache.getConfiguration());
      } catch (Exception e) {
         log.debug("isPassiveReplication method not found or can't be invoked. Assuming *no* passive replication in use");
      }
      return false;
   }

   //================================================= JMX STATS ====================================================

   private void tryResetStats(ObjectName component, MBeanServer mBeanServer) {
      Object[] emptyArgs = new Object[0];
      String[] emptySig = new String[0];
      try {
         log.trace("Try to reset stats in " + component);
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
      for (ObjectName name : mBeanServer.queryNames(null, null)) {
         if (name.getDomain().equals(domain)) {

            if ("Cache".equals(name.getKeyProperty("type"))) {
               String cacheName = name.getKeyProperty("name");
               String cacheManagerName = name.getKeyProperty("manager");
               return domain + ":type=Cache,name=" +
                     (cacheName.startsWith("\"") ? cacheName : ObjectName.quote(cacheName)) +
                     ",manager=" + (cacheManagerName.startsWith("\"") ? cacheManagerName :
                                          ObjectName.quote(cacheManagerName)) +
                     ",component=";
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

         String filePath = "top-keys-" + transport.getAddress();

         log.info("Collecting statistics from Stream Lib component [" + streamLibStats + "] and save them in " +
                        filePath);
         log.debug("Attributes available are " +
                         mBeanAttributes2String(mBeanServer.getMBeanInfo(streamLibStats).getAttributes()));

         BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));

         bufferedWriter.write("RemoteTopGets=" + getMapAttribute(mBeanServer, streamLibStats, "RemoteTopGets")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("LocalTopGets=" + getMapAttribute(mBeanServer, streamLibStats, "LocalTopGets")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("RemoteTopPuts=" + getMapAttribute(mBeanServer, streamLibStats, "RemoteTopPuts")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("LocalTopPuts=" + getMapAttribute(mBeanServer, streamLibStats, "LocalTopPuts")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("TopLockedKeys=" + getMapAttribute(mBeanServer, streamLibStats, "TopLockedKeys")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("TopContendedKeys=" + getMapAttribute(mBeanServer, streamLibStats, "TopContendedKeys")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("TopLockFailedKeys=" + getMapAttribute(mBeanServer, streamLibStats, "TopLockFailedKeys")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.write("TopWriteSkewFailedKeys=" + getMapAttribute(mBeanServer, streamLibStats, "TopWriteSkewFailedKeys")
               .toString());
         bufferedWriter.newLine();
         bufferedWriter.flush();
         bufferedWriter.close();

      } catch (Exception e) {
         log.warn("Unable to collect stats from Stream Lib Statistic component");
      }
   }

   private void getStatsFrom(String baseName, MBeanServer mBeanServer, Map<String, String> results,
                             StatisticComponent statisticComponent) {
      try {
         ObjectName objectName = new ObjectName(baseName + statisticComponent.getName());

         if (!mBeanServer.isRegistered(objectName)) {
            log.info("Not collecting statistics from [" + objectName + "]. It is not registered");
            return;
         }

         log.info("Collecting statistics from component [" + objectName + "]");
         log.debug("Attributes available are " +
                         mBeanAttributes2String(mBeanServer.getMBeanInfo(objectName).getAttributes()));
         log.trace("Attributes to be reported are " + statisticComponent.getStats());

         for (Map.Entry<String, String> entry : statisticComponent.getStats()) {
            results.put(entry.getKey(), getAsStringAttribute(mBeanServer, objectName, entry.getValue()));
         }
      } catch (Exception e) {
         log.warn("Unable to collect stats from Total Order Validator component");
      }
   }

   @SuppressWarnings("unchecked")
   private Map<Object, Object> getMapAttribute(MBeanServer mBeanServer, ObjectName component, String attr) {
      try {
         return (Map<Object, Object>) mBeanServer.getAttribute(component, attr);
      } catch (Exception e) {
         log.warn(String.format(GET_ATTRIBUTE_ERROR, attr, component));
         log.debug(e);
      }
      return Collections.emptyMap();
   }

   private String getAsStringAttribute(MBeanServer mBeanServer, ObjectName component, String attr) {
      try {
         return String.valueOf(mBeanServer.getAttribute(component, attr));
      } catch (Exception e) {
         log.warn(String.format(GET_ATTRIBUTE_ERROR, attr, component));
         log.debug(e);
      }
      return "Not_Available";
   }
}