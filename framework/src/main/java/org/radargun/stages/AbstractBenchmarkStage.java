package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.AbstractCacheWrapperStressor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Double.parseDouble;
import static org.radargun.utils.Utils.numberFormat;

/**
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public abstract class AbstractBenchmarkStage extends AbstractDistStage {

   //the number of threads that will work on this slave
   private int numOfThreads = 10;

   //indicates that the coordinator executes transactions or not
   private boolean coordinatorParticipation = true;

   //simulation time (in seconds)
   private long perThreadSimulTime;

   //the cache wrapper
   private CacheWrapper cacheWrapper;

   //the sampling interval
   private long statsSamplingInterval = 0;


   public DistStageAck executeOnSlave() {
      DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
      this.cacheWrapper = slaveState.getCacheWrapper();
      if (cacheWrapper == null) {
         log.info("Not running test on this slave as the wrapper hasn't been configured.");
         return result;
      }

      log.info("Starting WebSessionBenchmarkStage: " + this.toString());

      AbstractCacheWrapperStressor stressor = createStressor();
      stressor.setSlaveIdx(getSlaveIndex());
      stressor.setNumberOfNodes(getActiveSlaveCount());
      stressor.setSimulationTime(perThreadSimulTime);
      stressor.setNumOfThreads(numOfThreads);
      stressor.setCoordinatorParticipation(coordinatorParticipation);
      stressor.setStatsSamplingInterval(statsSamplingInterval);

      try {
         Map<String, String> results = stressor.stress(cacheWrapper);
         result.setPayload(results);
         return result;
      } catch (Exception e) {
         log.warn("Exception while initializing the test", e);
         result.setError(true);
         result.setRemoteException(e);
         result.setErrorMessage(e.getMessage());
         return result;
      }
   }

   @SuppressWarnings("unchecked")
   public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
      logDurationInfo(acks);
      boolean success = true;
      Map<Integer, Map<String, Object>> results = new HashMap<Integer, Map<String, Object>>();
      masterState.put("results", results);
      for (DistStageAck ack : acks) {
         DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
         if (wAck.isError()) {
            success = false;
            log.warn("Received error ack: " + wAck);
         } else {
            if (log.isTraceEnabled())
               log.trace(wAck);
         }
         Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
         if (benchResult != null) {
            results.put(ack.getSlaveIndex(), benchResult);
            Object reqPerSes = benchResult.get("TX_PER_SEC");
            if (reqPerSes == null) {
               throw new IllegalStateException("This should be there!");
            }
            log.info("On slave " + ack.getSlaveIndex() + " we had " + numberFormat(parseDouble(reqPerSes.toString())) + " requests per second");
         } else {
            log.trace("No report received from slave: " + ack.getSlaveIndex());
         }
      }
      return success;
   }

   @Override
   public String toString() {
      return "numOfThreads=" + numOfThreads +
            ", coordinatorParticipation=" + coordinatorParticipation +
            ", perThreadSimulTime=" + perThreadSimulTime +
            ", cacheWrapper=" + cacheWrapper +
            ", " + super.toString();
   }

   protected abstract AbstractCacheWrapperStressor createStressor();

   public void setPerThreadSimulTime(long perThreadSimulTime){
      this.perThreadSimulTime = perThreadSimulTime;
   }

   public void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
   }

   public void setCoordinatorParticipation(boolean coordinatorParticipation) {
      this.coordinatorParticipation = coordinatorParticipation;
   }

   public void setStatsSamplingInterval(long interval){
      this.statsSamplingInterval = interval;
   }
}
