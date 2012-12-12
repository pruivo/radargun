package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.workloads.KeyGeneratorFactory;
import org.radargun.workloads.PopulationEntry;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The warmup that initialize the keys used by the benchmark
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public abstract class AbstractPopulationStressor<T extends KeyGeneratorFactory> extends AbstractCacheWrapperStressor {

   //the log
   protected final Log log = LogFactory.getLog(getClass());
   //the key generator factory
   private final T factory;
   //the slave/node index
   private int nodeIndex = 0;
   //the size of the transaction. if size is less or equals 1, than it will be disable
   private int transactionSize = 100;

   public AbstractPopulationStressor() {
      factory = createFactory();
   }

   @Override
   public Map<String, String> stress(CacheWrapper wrapper) {
      if (wrapper == null) {
         throw new IllegalStateException("Null wrapper not allowed");
      }

      factory.calculate();

      Iterator<PopulationEntry> iterator;
      if (wrapper.isPassiveReplication() && wrapper.isTheMaster()) {
         iterator = factory.populateAll();
      } else if (!wrapper.isPassiveReplication()){
         iterator = factory.populateNode(nodeIndex);
      } else {
         return null;
      }

      if (isTransactional()) {
         performTransactionalWarmup(wrapper, iterator);
      } else {
         performNonTransactionalWarmup(wrapper, iterator);
      }

      return null;
   }

   @Override
   public final void destroy() throws Exception {
      //Do nothing... we don't want to loose the keys
   }

   @Override
   public String toString() {
      return "nodeIndex=" + nodeIndex +
            ", transactionSize=" + transactionSize +
            ", keyGeneratorFactory=" + factory +
            '}';
   }

   public final T getFactory() {
      return factory;
   }

   public final void setNodeIndex(int nodeIndex) {
      this.nodeIndex = nodeIndex;
   }

   public final void setNumberOfKeys(int numberOfKeys) {
      factory.setNumberOfKeys(numberOfKeys);
   }

   public final void setSizeOfValue(int sizeOfValue) {
      factory.setValueSize(sizeOfValue);
   }

   public final void setNumOfThreads(int numOfThreads) {
      factory.setNumberOfThreads(numOfThreads);
   }

   public final void setBucketPrefix(String bucketPrefix) {
      factory.setBucketPrefix(bucketPrefix);
   }

   public final void setTransactionSize(int transactionSize) {
      this.transactionSize = transactionSize;
   }

   public final void setNumberOfNodes(int numberOfNodes) {
      factory.setNumberOfNodes(numberOfNodes);
   }

   protected abstract T createFactory();

   private void performTransactionalWarmup(CacheWrapper cacheWrapper, Iterator<PopulationEntry> iterator) {
      if (log.isTraceEnabled()) {
         log.trace("Performing transactional warmup. Transaction size is " + transactionSize);
      }

      while (iterator.hasNext()) {
         List<PopulationEntry> populationEntryList = new LinkedList<PopulationEntry>();
         for (int i = 0; i < transactionSize && iterator.hasNext(); ++i) {
            populationEntryList.add(iterator.next());
         }

         boolean success = false;
         while (!success) {
            cacheWrapper.startTransaction();
            try {
               for (PopulationEntry populationEntry : populationEntryList) {
                  cacheWrapper.put(populationEntry.getBucket(), populationEntry.getKey(), populationEntry.getValue());
               }
               success = true;
            } catch (Exception e) {
               success = false;
               log.warn("A put operation has failed. retry the transaction");
            }
            try {
               cacheWrapper.endTransaction(success);
            } catch (Exception e) {
               log.warn("A transaction has rolled back. retry the transaction");
            }
         }
      }
   }

   private void performNonTransactionalWarmup(CacheWrapper cacheWrapper, Iterator<PopulationEntry> iterator) {
      while (iterator.hasNext()) {
         PopulationEntry populationEntry = iterator.next();
         boolean success = false;
         while (!success) {
            try {
               cacheWrapper.put(populationEntry.getBucket(), populationEntry.getKey(), populationEntry.getValue());
               success = true;
            } catch (Exception e) {
               log.warn("A put operation has failed. retry the put operation");
            }
         }
      }
   }

   private boolean isTransactional() {
      return transactionSize > 1;
   }
}
