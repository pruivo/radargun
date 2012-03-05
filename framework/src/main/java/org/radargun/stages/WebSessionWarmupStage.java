package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.PutGetWarmupStressor;

import java.util.List;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class WebSessionWarmupStage extends AbstractDistStage {

   //allows execution without contention
   private boolean noContentionEnabled = false;

   //for each there will be created fixed number of keys. All the GETs and PUTs are performed on these keys only.
   private int numberOfKeys = 1000;

   //Each key will be a byte[] of this size.
   private int sizeOfValue = 1000;

   //the number of threads that will work on this cache wrapper.
   private int numOfThreads = 1;

   private String bucketPrefix = null;

   @Override
   public DistStageAck executeOnSlave() {
      DefaultDistStageAck ack = newDefaultStageAck();
      CacheWrapper wrapper = slaveState.getCacheWrapper();
      if (wrapper == null) {
         log.info("Not executing any test as the wrapper is not set up on this slave ");
         return ack;
      }
      PutGetWarmupStressor putGetWarmupStressor = new PutGetWarmupStressor();
      putGetWarmupStressor.setSlaveIdx(this.slaveIndex);
      putGetWarmupStressor.setNoContentionEnabled(noContentionEnabled);
      putGetWarmupStressor.setNumberOfKeys(numberOfKeys);
      putGetWarmupStressor.setSizeOfValue(sizeOfValue);
      putGetWarmupStressor.setNumOfThreads(numOfThreads);
      putGetWarmupStressor.setBucketPrefix(bucketPrefix);

      long startTime = System.currentTimeMillis();
      putGetWarmupStressor.stress(wrapper);
      long duration = System.currentTimeMillis() - startTime;
      log.info("The init stage took: " + (duration / 1000) + " seconds.");
      ack.setPayload(duration);
      return ack;
   }

   public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
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
      return "WebSessionWarmupStage{" +
            "noContentionEnabled=" + noContentionEnabled +
            ", numberOfKeys=" + numberOfKeys +
            ", sizeOfValue=" + sizeOfValue +
            ", numOfThreads=" + numOfThreads +
            ", bucketPrefix='" + bucketPrefix + '\'' +
            '}';
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
}
