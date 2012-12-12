package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.AbstractBenchmarkStressor;
import org.radargun.workloads.KeyGeneratorFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Double.parseDouble;
import static org.radargun.utils.Utils.numberFormat;

/**
 * Simulates the work with a distributed web sessions.
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public abstract class AbstractBenchmarkStage extends AbstractDistStage {

   protected static final Integer NOT_CHANGED = -1;
   protected static final String NOT_CHANGED_S = "_#_";
   private static final String SCRIPT_LAUNCH = "_script_launch_";
   //for each there will be created fixed number of keys. All the GETs and PUTs are performed on these keys only.
   private int numberOfKeys = NOT_CHANGED;
   //Each key will be a byte[] of this size.
   private int sizeOfValue = NOT_CHANGED;
   //the number of threads that will work on this cache wrapper.
   private int numOfThreads = NOT_CHANGED;
   //the bucket prefix
   private String bucketPrefix = NOT_CHANGED_S;
   //simulation time (in seconds)
   private long perThreadSimulationTime = 30;
   //allows execution without contention
   private boolean noContention = false;
   //true if the coordinator should execute transactions
   private boolean coordinatorParticipation = true;
   //the probability of the key to be local, assuming key_x_?? is store in node x
   private int localityProbability = -1;
   //for gaussian keys
   private double stdDev = -1;
   //path for the script to start when the benchmark starts
   private String beforeScriptPath = null;

   @Override
   public final void initOnMaster(MasterState masterState, int slaveIndex) {
      super.initOnMaster(masterState, slaveIndex);
      Boolean started = (Boolean) masterState.get(SCRIPT_LAUNCH);
      if (started == null || !started) {
         masterState.put(SCRIPT_LAUNCH, startScript());
      }
   }

   @Override
   public final DistStageAck executeOnSlave() {
      DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
      CacheWrapper cacheWrapper = slaveState.getCacheWrapper();
      if (cacheWrapper == null) {
         log.info("Not running test on this slave as the wrapper hasn't been configured.");
         return result;
      }

      log.info("Starting WebSessionBenchmarkStage: " + this.toString());

      AbstractBenchmarkStressor stressor = createStressor(slaveState.getKeyGeneratorFactory());
      stressor.setNodeIndex(getSlaveIndex());
      stressor.setNumberOfNodes(getActiveSlaveCount());
      stressor.setSimulationTime(perThreadSimulationTime);
      stressor.setCoordinatorParticipation(coordinatorParticipation);
      stressor.setNoContention(noContention);
      stressor.setLocalityProbability(localityProbability);
      stressor.setStdDev(stdDev);

      if (bucketPrefix != NOT_CHANGED_S) {
         stressor.setBucketPrefix(bucketPrefix);
      }

      if (sizeOfValue != NOT_CHANGED) {
         stressor.setSizeOfValue(sizeOfValue);
      }

      if (numberOfKeys != NOT_CHANGED) {
         stressor.setNumberOfKeys(numberOfKeys);
      }

      if (numOfThreads != NOT_CHANGED) {
         stressor.setNumberOfThreads(numOfThreads);
      }

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

   @Override
   public final boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
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
      return "numberOfKeys=" + numberOfKeys +
            ", sizeOfValue=" + sizeOfValue +
            ", numOfThreads=" + numOfThreads +
            ", coordinatorParticipation=" + coordinatorParticipation +
            ", bucketPrefix='" + bucketPrefix + '\'' +
            ", perThreadSimulationTime=" + perThreadSimulationTime +
            ", noContention=" + noContention +
            ", localityProbability=" + localityProbability +
            ", stdDev=" + stdDev +
            ", " + super.toString();
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

   public final void setPerThreadSimulationTime(long perThreadSimulationTime) {
      this.perThreadSimulationTime = perThreadSimulationTime;
   }

   public final void setNoContention(boolean noContention) {
      this.noContention = noContention;
   }

   public final void setCoordinatorParticipation(boolean coordinatorParticipation) {
      this.coordinatorParticipation = coordinatorParticipation;
   }

   public final void setLocalityProbability(int localityProbability) {
      this.localityProbability = localityProbability;
   }

   public final void setStdDev(double stdDev) {
      this.stdDev = stdDev;
   }

   public final void setBeforeScriptPath(String beforeScriptPath) {
      this.beforeScriptPath = beforeScriptPath;
   }

   protected abstract AbstractBenchmarkStressor createStressor(KeyGeneratorFactory keyGeneratorFactory);

   private Boolean startScript() {
      if (beforeScriptPath == null) {
         return Boolean.TRUE;
      }
      try {
         Runtime.getRuntime().exec(beforeScriptPath);
         log.info("Script " + beforeScriptPath + " started successfully");
         return Boolean.TRUE;
      } catch (Exception e) {
         log.warn("Error starting script " + beforeScriptPath + ". " + e.getMessage());
         return Boolean.FALSE;
      }
   }
}
