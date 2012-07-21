package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedAttribute;
import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.keygenerator.KeyGenerator.KeyGeneratorFactory;
import org.radargun.state.MasterState;
import org.radargun.stressors.PutGetStressor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Double.parseDouble;
import static org.radargun.utils.Utils.numberFormat;

/**
 * Simulates the work with a distributed web sessions.
 *
 * @author Mircea.Markus@jboss.com
 */
@MBean(objectName = "BenchmarkStage", description = "The benchmark stage")
public class WebSessionBenchmarkStage extends AbstractDistStage {

   private static final String SCRIPT_LAUNCH = "_script_launch_";
   private static final String SCRIPT_PATH = "/home/pruivo/beforeBenchmark.sh";

   //for each session there will be created fixed number of attributes. On those attributes all the GETs and PUTs are
   // performed (for PUT is overwrite)
   private int numberOfKeys = 10000;

   //Each attribute will be a byte[] of this size
   private int sizeOfValue = 1000;

   //Out of the total number of request, this define the frequency of writes (percentage)
   private int writeOperationPercentage = 20;

   //The percentage of write transactions generated
   private int writeTransactionPercentage = 100;

   //the number of threads that will work on this slave
   private int numOfThreads = 10;

   //indicates that the coordinator executes transactions or not
   private boolean coordinatorParticipation = true;

   //needed to compute the uniform distribution of operations per transaction
   private int lowerBoundOp = 100;

   // as above
   private int upperBoundOp = 100;

   //simulation time (in seconds)
   private long perThreadSimulTime;

   //allows execution without contention
   private boolean noContentionEnabled = false;

   private CacheWrapper cacheWrapper;
   private int opsCountStatusLog = 5000;
   private boolean reportNanos = false;

   private long statsSamplingInterval = 0;

   private transient PutGetStressor stressor;

   @Override
   public void initOnMaster(MasterState masterState, int slaveIndex) {
      super.initOnMaster(masterState, slaveIndex);
      Boolean started = (Boolean) masterState.get(SCRIPT_LAUNCH);
      if (started == null || !started) {
         masterState.put(SCRIPT_LAUNCH, startScript());
      }
   }

   private Boolean startScript() {
      try {
         Runtime.getRuntime().exec(SCRIPT_PATH);
         log.info("Script " + SCRIPT_PATH + " started successfully");
         return Boolean.TRUE;
      } catch (Exception e) {
         log.warn("Error starting script " + SCRIPT_PATH + ". " + e.getMessage());
         return Boolean.FALSE;
      }
   }

   public DistStageAck executeOnSlave() {
      DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
      this.cacheWrapper = slaveState.getCacheWrapper();
      if (cacheWrapper == null) {
         log.info("Not running test on this slave as the wrapper hasn't been configured.");
         return result;
      }

      log.info("Starting WebSessionBenchmarkStage: " + this.toString());

      stressor = new PutGetStressor();
      stressor.setSlaveIdx(getSlaveIndex());
      stressor.setNumberOfNodes(getActiveSlaveCount());
      stressor.setLowerBoundOp(lowerBoundOp);
      stressor.setUpperBoundOp(upperBoundOp);
      stressor.setSimulationTime(perThreadSimulTime);
      stressor.setBucketPrefix(getSlaveIndex() + "");
      stressor.setNumberOfKeys(numberOfKeys);
      stressor.setNumOfThreads(numOfThreads);
      stressor.setOpsCountStatusLog(opsCountStatusLog);
      stressor.setSizeOfValue(sizeOfValue);
      stressor.setWriteOperationPercentage(writeOperationPercentage);
      stressor.setWriteTransactionPercentage(writeTransactionPercentage);
      stressor.setCoordinatorParticipation(coordinatorParticipation);
      stressor.setNoContentionEnabled(noContentionEnabled);
      stressor.setStatsSamplingInterval(statsSamplingInterval);
      stressor.setFactory((KeyGeneratorFactory) slaveState.get("key_gen_factory"));

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
      return "WebSessionBenchmarkStage {" +
            "opsCountStatusLog=" + opsCountStatusLog +
            ", numberOfKeys=" + numberOfKeys +
            ", sizeOfValue=" + sizeOfValue +
            ", writeOperationPercentage=" + writeOperationPercentage +
            ", writeTransactionPercentage=" + writeTransactionPercentage +
            ", numOfThreads=" + numOfThreads +
            ", reportNanos=" + reportNanos +
            ", coordinatorParticipation=" + coordinatorParticipation +
            ", lowerBoundOp=" + lowerBoundOp +
            ", upperBoundOp=" + upperBoundOp +
            ", perThreadSimulTime=" + perThreadSimulTime +
            ", noContentionEnabled=" + noContentionEnabled +
            ", cacheWrapper=" + cacheWrapper +
            ", " + super.toString();
   }

   @ManagedOperation
   public void setLowerBoundOp(int lb){
      this.lowerBoundOp=lb;
      if (stressor != null) {
         stressor.setLowerBoundOp(lb);
      }
   }

   @ManagedOperation
   public void setUpperBoundOp(int ub){
      this.upperBoundOp=ub;
      if (stressor != null) {
         stressor.setUpperBoundOp(ub);
      }
   }

   public void setPerThreadSimulTime(long perThreadSimulTime){
      this.perThreadSimulTime = perThreadSimulTime;
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

   public void setReportNanos(boolean reportNanos) {
      this.reportNanos = reportNanos;
   }

   @ManagedOperation
   public void setWriteOperationPercentage(int writeOperationPercentage) {
      this.writeOperationPercentage = writeOperationPercentage;
      if (stressor != null) {
         stressor.setWriteOperationPercentage(writeOperationPercentage);
      }
   }

   public void setOpsCountStatusLog(int opsCountStatusLog) {
      this.opsCountStatusLog = opsCountStatusLog;
   }

   public void setCoordinatorParticipation(boolean coordinatorParticipation) {
      this.coordinatorParticipation = coordinatorParticipation;
   }

   public void setNoContentionEnabled(boolean noContentionEnabled) {
      this.noContentionEnabled = noContentionEnabled;
   }

   @ManagedOperation
   public void setWriteTransactionPercentage(int writeTransactionPercentage) {
      this.writeTransactionPercentage = writeTransactionPercentage;
      if (stressor != null) {
         stressor.setWriteTransactionPercentage(writeTransactionPercentage);
      }
   }

   public void setStatsSamplingInterval(long interval){
      this.statsSamplingInterval = interval;
   }

   @ManagedOperation
   public void stop() {
      stressor.stopBenchmark();
   }

   @ManagedAttribute
   public int getWriteOperationPercentage() {
      return writeOperationPercentage;
   }

   @ManagedAttribute
   public int getExpectedWritePercentage() {
      return writeTransactionPercentage;
   }

   @ManagedAttribute
   public int getUpperBoundOp() {
      return upperBoundOp;
   }

   @ManagedAttribute
   public int getLowerBoundOp() {
      return lowerBoundOp;
   }

   @ManagedAttribute
   public int getNumberOfActiveThreads() {
      return numOfThreads;
   }
}
