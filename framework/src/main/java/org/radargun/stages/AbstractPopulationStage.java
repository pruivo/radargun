package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.AbstractPopulationStressor;

import java.util.List;

import static org.radargun.utils.Utils.getMillisDurationString;

/**
 * Performs the population in the cache, initializing all the keys used by benchmarking
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public abstract class AbstractPopulationStage extends AbstractDistStage {

   //for each there will be created fixed number of keys. All the GETs and PUTs are performed on these keys only.
   private int numberOfKeys = 1000;
   //Each key will be a byte[] of this size.
   private int sizeOfValue = 1000;
   //the number of threads that will work on this cache wrapper.
   private int numOfThreads = 1;
   //the bucket prefix
   private String bucketPrefix = null;
   //the size of the transaction. if size is less or equals 1, than it will be disable
   private int transactionSize = 100;

   @Override
   public final DistStageAck executeOnSlave() {
      DefaultDistStageAck ack = newDefaultStageAck();
      CacheWrapper wrapper = slaveState.getCacheWrapper();
      if (wrapper == null) {
         log.info("Not executing any test as the wrapper is not set up on this slave ");
         return ack;
      }

      AbstractPopulationStressor populationStressor = createPopulationStressor();
      populationStressor.setNodeIndex(this.slaveIndex);
      populationStressor.setNumberOfKeys(numberOfKeys);
      populationStressor.setSizeOfValue(sizeOfValue);
      populationStressor.setNumOfThreads(numOfThreads);
      populationStressor.setBucketPrefix(bucketPrefix);
      populationStressor.setNumberOfNodes(getActiveSlaveCount());
      populationStressor.setTransactionSize(transactionSize);

      long startTime = System.currentTimeMillis();
      populationStressor.stress(wrapper);
      long duration = System.currentTimeMillis() - startTime;
      log.info("The init stage took: " + getMillisDurationString(duration) + " seconds.");
      ack.setPayload(duration);

      slaveState.setKeyGeneratorFactory(populationStressor.getFactory());
      return ack;
   }

   @Override
   public final boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
      logDurationInfo(acks);
      for (DistStageAck ack : acks) {
         DefaultDistStageAck dAck = (DefaultDistStageAck) ack;
         if (log.isTraceEnabled()) {
            log.trace("Init on slave " + dAck.getSlaveIndex() + " finished in " + dAck.getPayload() + " millis.");
         }
      }
      return true;
   }

   @Override
   public String toString() {
      return "numberOfKeys=" + numberOfKeys +
            ", sizeOfValue=" + sizeOfValue +
            ", numOfThreads=" + numOfThreads +
            ", transactionSize=" + transactionSize +
            ", bucketPrefix='" + bucketPrefix + '\'' +
            '}';
   }

   public final void setNumberOfKeys(int numberOfKeys) {
      this.numberOfKeys = numberOfKeys;
   }

   public final void setSizeOfValue(int sizeOfValue) {
      this.sizeOfValue = sizeOfValue;
   }

   public final void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
   }

   public final void setBucketPrefix(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix;
   }

   public final void setTransactionSize(int transactionSize) {
      this.transactionSize = transactionSize;
   }

   protected abstract AbstractPopulationStressor createPopulationStressor();
}
