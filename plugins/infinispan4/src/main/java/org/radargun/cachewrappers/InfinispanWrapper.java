package org.radargun.cachewrappers;

import com.arjuna.ats.arjuna.common.arjPropertyManager;
import com.arjuna.ats.internal.arjuna.objectstore.VolatileStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.TimeoutException;
import org.radargun.CacheWrapper;
import org.radargun.cachewrappers.parser.StatisticComponent;
import org.radargun.cachewrappers.parser.StatsParser;
import org.radargun.utils.TypedProperties;
import org.radargun.utils.Utils;

import javax.management.*;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.radargun.utils.Utils.mBeanAttributes2String;
import static org.radargun.utils.Utils.printMemoryFootprint;

public class InfinispanWrapper implements CacheWrapper {
   private static final String GET_ATTRIBUTE_ERROR = "Exception while obtaining the attribute [%s] from [%s]";
   private final Set<Object> newKeys = new ConcurrentSkipListSet<Object>();
   private static final int MAX_THREADS = 100;
   private final List<Object>[] perThreadNewKeys = (LinkedList<Object>[]) new LinkedList[MAX_THREADS];
   private boolean trackNewKeys = false;
   private boolean perThreadTrackNewKeys = false;
   private static final int maxSleep = 2000;
   private static final boolean takeAllStats = false;


   static {
      // Set up transactional stores for JBoss TS
      arjPropertyManager.getCoordinatorEnvironmentBean().setCommunicationStore(VolatileStore.class.getName());
      arjPropertyManager.getObjectStoreEnvironmentBean().setObjectStoreType(VolatileStore.class.getName());
      arjPropertyManager.getCoordinatorEnvironmentBean().setDefaultTimeout(300); //300 seconds == 5 min
   }

   private static Log log = LogFactory.getLog(InfinispanWrapper.class);
   DefaultCacheManager cacheManager;
   private Cache<Object, Object> cache;
   private Cache<Object, Object> writeCache;
   TransactionManager tm;
   boolean started = false;
   String config;
   private volatile boolean enlistExtraXAResource;
   Transport transport;
   Method isPassiveReplicationMethod = null;

   private List<StatisticComponent> statisticComponents;

   public void setUp(String config, boolean isLocal, int nodeIndex, TypedProperties confAttributes) throws Exception {
      this.config = config;
      String configFile = confAttributes.containsKey("file") ? confAttributes.getProperty("file") : config;
      String cacheName = confAttributes.containsKey("cache") ? confAttributes.getProperty("cache") : "x";

      log.trace("Using config file: " + configFile + " and cache name: " + cacheName);

      if (!started) {
         cacheManager = new DefaultCacheManager(configFile);
         String cacheNames = cacheManager.getDefinedCacheNames();
         if (!cacheNames.contains(cacheName))
            throw new IllegalStateException("The requested cache(" + cacheName + ") is not defined. Defined cache " +
                                                  "names are " + cacheNames);
         cache = cacheManager.getCache(cacheName);
         writeCache = cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
         started = true;
         tm = cache.getAdvancedCache().getTransactionManager();
         log.info("Using transaction manager: " + tm);
         //Changed to comply with 5.0 API
         transport = cache.getAdvancedCache().getRpcManager().getTransport();
         try {
            isPassiveReplicationMethod = Configuration.class.getMethod("isPassiveReplication");
         } catch (Exception e) {
            //just ignore
            isPassiveReplicationMethod = null;
         }
      }
      log.debug("Loading JGroups from: " + org.jgroups.Version.class.getProtectionDomain().getCodeSource().getLocation());
      log.info("JGroups version: " + org.jgroups.Version.printDescription());
      log.info("Using config attributes: " + confAttributes);
      blockForRehashing();
      //log.warn("Beware! Relying on the default hash function");
      // injectEvenConsistentHash(confAttributes);
      //injectEvenConsistentHash(confAttributes);


      for (int i = 0; i < MAX_THREADS; i++) {
         this.perThreadNewKeys[i] = new LinkedList<Object>();
      }
   }

   public void tearDown() throws Exception {
      List<Address> addressList = cacheManager.getMembers();
      if (started) {
         cacheManager.stop();
         log.trace("Stopped, previous view is " + addressList);
         started = false;
      }
   }


   public List<Address> members() {
      return cache.getAdvancedCache().getRpcManager().getTransport().getMembers();
   }

   public void put(String bucket, Object key, Object value) throws Exception {

      writeCache.put(key,value);
      /*
      try {
         if (cache.put(key, value) == null && this.trackNewKeys)
            this.newKeys.add(key);
      } catch (Exception e) {
         log.warn(e.getMessage());
         log.warn("Error on key " + key);
         throw e;
      }
      */
   }

   @Override
   public void putIfLocal(String bucket, Object key, Object value) throws Exception {
      AdvancedCache<Object, Object> advancedCache = cache.getAdvancedCache();
      DistributionManager distributionManager = advancedCache.getDistributionManager();
      //Changed to comply with 5.0 API
      if (distributionManager == null || distributionManager.isLocal(key)) {
         advancedCache.withFlags(Flag.CACHE_MODE_LOCAL).put(key, value);
      }
   }

   public Object get(String bucket, Object key) throws Exception {
      return cache.get(key);
   }

   public void empty() throws Exception {
      RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
      int clusterSize = 0;
      if (rpcManager != null) {
         clusterSize = rpcManager.getTransport().getMembers().size();
      }
      //use keySet().size() rather than size directly as cache.size might not be reliable
      log.info("Cache size before clear (cluster size= " + clusterSize + ")" + cache.keySet().size());

      cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
      log.info("Cache size after clear: " + cache.keySet().size());
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
      //Important: don't change this string without validating the ./dist.sh as it relies on its format!!
      return "Running : " + cache.getVersion() + ", config:" + config + ", cacheName:" + cache.getName();
   }

   public Object getReplicatedData(String bucket, String key) throws Exception {
      return get(bucket, key);
   }

   public void startTransaction() {
      assertTm();

      try {
         tm.begin();
         Transaction transaction = tm.getTransaction();
         if (enlistExtraXAResource) {
            transaction.enlistResource(new DummyXAResource());
         }
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void startTransaction(boolean isReadOnly) {
      this.startTransaction();
   }


   public void endTransaction(boolean successful) {
      assertTm();
      try {
         if (successful) {
            //cache.getAdvancedCache().getComponentRegistry().getComponent(TransactionTable.class).getLocalTransaction(tm.getTransaction());
            tm.commit();
         } else
            tm.rollback();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public boolean isInTransaction() {
      try {
         return tm != null && tm.getStatus() != Status.STATUS_NO_TRANSACTION;
      } catch (SystemException e) {
         //
      }
      return false;
   }

   private void blockForRehashing() throws InterruptedException {
      // should we be blocking until all rehashing, etc. has finished?
      long gracePeriod = MINUTES.toMillis(15);
      long giveup = System.currentTimeMillis() + gracePeriod;
      if (cache.getConfiguration().getCacheMode().isDistributed()) {
         while (!cache.getAdvancedCache().getDistributionManager().isJoinComplete() && System.currentTimeMillis() < giveup)
            Thread.sleep(200);
      }

      if (cache.getConfiguration().getCacheMode().isDistributed() && !cache.getAdvancedCache().getDistributionManager().isJoinComplete())
         throw new RuntimeException("Caches haven't discovered and joined the cluster even after " + Utils.prettyPrintMillis(gracePeriod));
   }

   /*
   private void injectEvenConsistentHash(TypedProperties confAttributes) {

      if (cache.getConfiguration().getCacheMode().isDistributed()) {
         ConsistentHash ch = cache.getAdvancedCache().getDistributionManager().getConsistentHash();
         if (ch instanceof EvenSpreadingConsistentHash) {
            int threadsPerNode = confAttributes.getIntProperty("threadsPerNode", -1);
            if (threadsPerNode < 0)
               throw new IllegalStateException("When EvenSpreadingConsistentHash is used threadsPerNode must also be set.");
            int keysPerThread = confAttributes.getIntProperty("keysPerThread", -1);
            if (keysPerThread < 0)
               throw new IllegalStateException("When EvenSpreadingConsistentHash is used must also be set.");
            ((EvenSpreadingConsistentHash) ch).init(threadsPerNode, keysPerThread);
            log.info("Using an even consistent hash!");
         }

      }
   }


    private void injectEvenConsistentHash(TypedProperties confAttributes) {
        if(cache.getCacheConfiguration().clustering().cacheMode().isDistributed()){
            ConsistentHash ch = cache.getAdvancedCache().getDistributionManager().getConsistentHash();
            if (ch instanceof UniformContendedStringHash) {
                log.warn("Starting my hash");
                ch.setCaches(new HashSet(this.members()));
            }
            else{
                log.warn("ConsistentHash of class "+ch);
            }

        }
    }
   */
   private void assertTm() {
      if (tm == null) throw new RuntimeException("No configured TM!");
   }

   public void setEnlistExtraXAResource(boolean enlistExtraXAResource) {
      this.enlistExtraXAResource = enlistExtraXAResource;
   }

   @Override
   public int getCacheSize() {
      return cache.size();
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

   /**
    * unchecked*
    */
   @Deprecated
   private Map<String, String> getAllStats() {
      log.warn("Going to dump all the stats I can");
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      String domain = cacheManager.getGlobalConfiguration().getJmxDomain();
      Map<String, String> attrMap = new HashMap<String, String>();
      for (ObjectName name : mBeanServer.queryNames(null, null)) {
         log.info("Querying " + name);
         if (name.getDomain().equals(domain)) {
            try {
               MBeanInfo info = mBeanServer.getMBeanInfo(name);
               for (MBeanAttributeInfo attr : info.getAttributes()) {
                  try {
                     Object o = mBeanServer.getAttribute(name, attr.getName());
                     log.trace(name + " - " + attr.getName() + " - " + o);
                     if (o == null) {
                        log.warn(attr.getName() + " is not available!");
                        o = "Not_Available";
                     }
                     attrMap.put(attr.getName(), o.toString());
                  } catch (MBeanException e) {
                     e.printStackTrace();
                  } catch (AttributeNotFoundException e) {
                     e.printStackTrace();
                  }
               }
            } catch (InstanceNotFoundException e) {
               e.printStackTrace();
            } catch (IntrospectionException e) {
               e.printStackTrace();
            } catch (ReflectionException e) {
               e.printStackTrace();
            }

         }
         log.warn("Discarding " + name + " as it does not have domain " + domain);
      }
      return attrMap;
   }


   @Override
   public Map<String, String> getAdditionalStats() {
      if (takeAllStats) {
         try {
            return getAllStats();
         } catch (Exception e) {
            log.warn("ARGH");
            e.printStackTrace();
            return new HashMap<String, String>();     //empty
         }
      }
      Map<String, String> results = new HashMap<String, String>();
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      String cacheComponentString = getCacheComponentBaseString(mBeanServer);
      if (cacheComponentString != null) {
         saveStatsFromStreamLibStatistics(cacheComponentString, mBeanServer);
         statisticComponents = StatsParser.parse("all-stats.xml");
         if (statisticComponents != null) {
            for (StatisticComponent statisticComponent : statisticComponents) {
               getStatsFrom(cacheComponentString, mBeanServer, results, statisticComponent);
            }
         }
      } else {
         log.info("Not collecting additional stats. Infinispan MBeans not found");
      }
      return results;
   }

   @Override
   public boolean isPassiveReplication() {
      //TODO handle the case for v5!!!
        /*
        try {
            //DIEGO's
            return this.cache.getAdvancedCache().getConfiguration().isPassiveReplication();
        } catch (Exception e) {
        */
      try {
         //PEDRO's
         return isPassiveReplicationMethod != null && (isPassiveReplicationWithSwitch() ||
                                                             (Boolean) isPassiveReplicationMethod.invoke(cache.getConfiguration()));
      } catch (Exception ee) {
         log.debug("isPassiveReplication method not found or can't be invoked. Assuming *no* passive replication in use");
      }
      //}

      return false;

   }

   @Override
   public boolean isTheMaster() {
      return !isPassiveReplication() || transport.isCoordinator();
   }

   private boolean isPassiveReplicationWithSwitch() {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      String cacheComponentString = getCacheComponentBaseString(mBeanServer);

      if (cacheComponentString != null) {
         try {
            return "PB".equals(getAsStringAttribute(mBeanServer,
                                                    new ObjectName(cacheComponentString + "ReconfigurableReplicationManager"),
                                                    "currentProtocolId"));
         } catch (Exception e) {
            log.warn("Unable to check for Passive Replication protocol");
         }
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
         log.warn("resetStatistics invoked on " + component);
         return;
      } catch (Exception e) {
         log.debug("resetStatistics not found in " + component);
      }
      try {
         mBeanServer.invoke(component, "resetStats", emptyArgs, emptySig);
         log.warn("resetStats invoked on " + component);
         return;
      } catch (Exception e) {
         log.debug("resetStats not found in " + component);
      }
      try {
         mBeanServer.invoke(component, "reset", emptyArgs, emptySig);
         log.warn("reset invoked on " + component);
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
         for (ObjectName n : mBeanServer.queryNames(null, null))
            log.warn(n.toString());
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

   @Override
   public boolean isTimeoutException(Throwable t) {
      return t instanceof TimeoutException || t.getCause() instanceof TimeoutException;
   }

   @Override
   public void setTrackNewKeys(boolean b) {
      log.info("Setting trackNewKeys to " + b);
      this.trackNewKeys = b;
   }

   public void setPerThreadTrackNewKeys(boolean b) {
      log.info("Setting perThreadTrackNewKeys to " + b);
      this.perThreadTrackNewKeys = b;
   }

   @Override
   public void eraseNewKeys(int batchSize) {
      Iterator<Object> it = this.newKeys.iterator();
      int removedKeys = 0;
      log.warn(this.newKeys.size() + " newKey entries in the toErase list.");
      printMemoryFootprint(true);
      do {
         removedKeys += eraseInBatch(batchSize, it);
      }
      while (it.hasNext());
      printMemoryFootprint(false);
      log.warn(removedKeys + " newKey entries removed from the list (either by me or by anyone else in the system).");
      this.newKeys.clear();
   }

   private int eraseInBatch(int batchSize, Iterator<Object> iterator) {
      int i = 0, toSleep = 100, reallyRemoved = 0, removed = 0;
      boolean success;
      Set<Object> setToErase = new HashSet<Object>();
      //Populate the set of the keys to be erased in this batch
      while (i++ < batchSize && iterator.hasNext())
         setToErase.add(iterator.next());

      Iterator<Object> eraseIterator;
      do {
         eraseIterator = setToErase.iterator();
         this.startTransaction();
         success = true;
         reallyRemoved = 0;

         try {
            while (eraseIterator.hasNext()) {
               this.cache.remove(eraseIterator.next());
               reallyRemoved++;
            }
         } catch (Throwable t) {
            log.warn(t.getMessage());
            success = false;
            //If I have a local conflict (only LR since I am with 1 thread)
            //I can assume that the guy who's holding the contended key remotely will remove it from the cache
            //I remove it from the batch erase list
            eraseIterator.remove();
            removed++;
         }
         try {
            this.endTransaction(success);    //no local aborts
         } catch (Throwable t) {
            //If I experience a RR conflict, then I have to rely on some sort of backoff to "ensure" the progress
            log.warn("Could not commit my eraseKey batch. Backing off for " + toSleep + " msecs. Cause was " + t.getMessage());
            toSleep = sleepForAWhile(toSleep);
            success = false;
         }
      }
      while (!success);
      removed += reallyRemoved;
      return removed;
   }

   //Sleep for at least one msec
   private int sleepForAWhile(int toSleep) {

      try {
         Thread.sleep(toSleep);
      } catch (InterruptedException e) {
         System.exit(-1);
      }
      return (int) (1 + (maxSleep * Math.random()));
   }


   @Override
   public void put(String bucket, Object key, Object value, int threadId) throws Exception {

      writeCache.put(key,value);
      /*
      if (perThreadTrackNewKeys) {
         if (cache.put(key, value) == null) {
            this.perThreadNewKeys[threadId].add(key);
         }
      } else
         put(bucket, key, value);
         */
   }

   @Override
   public void endTransaction(boolean successful, int threadId) throws Exception {
      boolean innerSux = successful;
      try {
         this.endTransaction(successful);
      } catch (Exception e) {
         innerSux = false;
         throw e;
      } finally {
         if (perThreadTrackNewKeys) {
            if (innerSux) {
               this.newKeys.addAll(this.perThreadNewKeys[threadId]);
            }
            this.perThreadNewKeys[threadId].clear();
         }
      }
   }

   @Override
   public boolean isCoordinator() {
      return transport.isCoordinator();
   }
}
