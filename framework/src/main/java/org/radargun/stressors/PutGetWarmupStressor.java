package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.keygenerator.WarmupIterator;

import javax.transaction.RollbackException;
import java.util.List;
import java.util.Map;

import static org.radargun.keygenerator.KeyGenerator.KeyGeneratorFactory;

/**
 * The warmup that initialize the keys used by the benchmark
 *
 * @author pruivo
 * @since 4.0
 */
public class PutGetWarmupStressor implements CacheWrapperStressor {

   private static Log log = LogFactory.getLog(PutGetWarmupStressor.class);

   //the slave/node index
   private int slaveIdx = 0;

   //allows execution without contention
   private boolean noContentionEnabled = false;

   //for each there will be created fixed number of keys. All the GETs and PUTs are performed on these keys only.
   private int numberOfKeys = 1000;

   //Each key will be a byte[] of this size.
   private int sizeOfValue = 1000;

   //the number of threads that will work on this cache wrapper.
   private int numOfThreads = 1;

   private String bucketPrefix = null;

   //true if the cache wrapper uses passive replication    
   private boolean isPassiveReplication = false;

   //the size of the transaction. if size is less or equals 1, than it will be disable
   private int transactionSize = 100;

   //the number of nodes
   private int numberOfNodes = 1;

   private KeyGeneratorFactory factory;

   @Override
   public Map<String, String> stress(CacheWrapper wrapper) {
      if (wrapper == null) {
         throw new IllegalStateException("Null wrapper not allowed");
      }

      factory = new KeyGeneratorFactory(numberOfNodes, numOfThreads, numberOfKeys, sizeOfValue,
                                        !noContentionEnabled, bucketPrefix);

      List<WarmupIterator> iterators;
      if (isPassiveReplication) {
         iterators = factory.constructForWarmup(wrapper.canExecuteWriteTransactions(), true);
      } else {
         iterators = factory.constructForWarmup(slaveIdx, true);
      }

      log.trace("Warmup iterators created are " + iterators);

      if (isTransactional()) {
         for (WarmupIterator warmupIterator : iterators) {
            performTransactionalWarmup(wrapper, warmupIterator);
         }
      } else {
         for (WarmupIterator warmupIterator : iterators) {
            performNonTransactionalWarmup(wrapper, warmupIterator);
         }
      }

      return null;
   }

   private void performTransactionalWarmup(CacheWrapper cacheWrapper, WarmupIterator warmupIterator) {
      log.trace("Performing transactional warmup. Transaction size is " + transactionSize);
      while (warmupIterator.hasNext()) {
         warmupIterator.setCheckpoint();
         cacheWrapper.startTransaction();
         boolean success;
         try {
            for (int i = 0; i < transactionSize && warmupIterator.hasNext(); ++i) {
               cacheWrapper.put(warmupIterator.getBucketPrefix(), warmupIterator.next(), warmupIterator.getRandomValue());
            }
            success = true;
         } catch (Exception e) {
            warmupIterator.returnToLastCheckpoint();
            success = false;
            log.warn("A put operation has failed. retry the transaction");
         }
         try {
            cacheWrapper.endTransaction(success);
         } catch (RollbackException e) {
            log.warn("A transaction has rolled back. retry the transaction");
            warmupIterator.returnToLastCheckpoint();
         }
      }
   }

   private void performNonTransactionalWarmup(CacheWrapper cacheWrapper, WarmupIterator warmupIterator) {
      while (warmupIterator.hasNext()) {
         warmupIterator.setCheckpoint();
         try {
            cacheWrapper.put(warmupIterator.getBucketPrefix(), warmupIterator.next(), warmupIterator.getRandomValue());
         } catch (Exception e) {
            log.warn("A put operation has failed. retry the put operation");
            warmupIterator.returnToLastCheckpoint();
         }
      }
   }

   @Override
   public void destroy() throws Exception {
      //Do nothing... we don't want to loose the keys
   }

   @Override
   public String toString() {
      return "PutGetWarmupStressor{" +
            "slaveIdx=" + slaveIdx +
            ", noContentionEnabled=" + noContentionEnabled +
            ", numberOfKeys=" + numberOfKeys +
            ", sizeOfValue=" + sizeOfValue +
            ", numOfThreads=" + numOfThreads +
            ", bucketPrefix='" + bucketPrefix + '\'' +
            ", isPassiveReplication=" + isPassiveReplication +
            ", transactionSize=" + transactionSize +
            ", numberOfNodes=" + numberOfNodes +
            '}';
   }

   private boolean isTransactional() {
      return transactionSize > 1;
   }

   public KeyGeneratorFactory getFactory() {
      return factory;
   }

   public void setSlaveIdx(int slaveIdx) {
      this.slaveIdx = slaveIdx;
   }

   public void setNoContentionEnabled(boolean noContentionEnabled) {
      this.noContentionEnabled = noContentionEnabled;
   }

   public void setNumberOfKeys(int numberOfKeys) {
      this.numberOfKeys = numberOfKeys;
   }

   public void setSizeOfValue(int sizeOfValue) {
      this.sizeOfValue = sizeOfValue;
   }

   public void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
   }

   public void setBucketPrefix(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix;
   }

   public void setPassiveReplication(boolean passiveReplication) {
      isPassiveReplication = passiveReplication;
   }

   public void setTransactionSize(int transactionSize) {
      this.transactionSize = transactionSize;
   }

   public void setNumberOfNodes(int numberOfNodes) {
      this.numberOfNodes = numberOfNodes;
   }
}
