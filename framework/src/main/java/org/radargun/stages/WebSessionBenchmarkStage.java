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

   //The percentage of write transactions generated
   private int writeTransactionPercentage = 100;

   //the number of threads that will work on this slave
   private int numOfThreads = 10;

   //indicates that the coordinator executes transactions or not
   private boolean coordinatorParticipation = true;

   private String wrtOpsPerWriteTx = "100";
   
   private String rdOpsPerWriteTx = "100";
   
   private String rdOpsPerReadTx = "100";

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
      stressor.setWrtOpsPerWriteTx(wrtOpsPerWriteTx);
      stressor.setRdOpsPerWriteTx(rdOpsPerWriteTx);
      stressor.setRdOpsPerReadTx(rdOpsPerReadTx);
      stressor.setSimulationTime(perThreadSimulTime);
      stressor.setBucketPrefix(getSlaveIndex() + "");
      stressor.setNumberOfKeys(numberOfKeys);
      stressor.setNumOfThreads(numOfThreads);
      stressor.setOpsCountStatusLog(opsCountStatusLog);
      stressor.setSizeOfValue(sizeOfValue);      
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
            ", writeTransactionPercentage=" + writeTransactionPercentage +
            ", numOfThreads=" + numOfThreads +
            ", reportNanos=" + reportNanos +
            ", coordinatorParticipation=" + coordinatorParticipation +
            ", wrtOpsPerWriteTx=" + wrtOpsPerWriteTx +
            ", rdOpsPerWriteTx=" + rdOpsPerWriteTx +
            ", rdOpsPerReadTx=" + rdOpsPerReadTx +
            ", perThreadSimulTime=" + perThreadSimulTime +
            ", noContentionEnabled=" + noContentionEnabled +
            ", cacheWrapper=" + cacheWrapper +
            ", " + super.toString();
   }

   @ManagedOperation
   public void setWrtOpsPerWriteTx(String wrtOpsPerWriteTx) {
      this.wrtOpsPerWriteTx = wrtOpsPerWriteTx;
   }

   @ManagedOperation
   public void setRdOpsPerWriteTx(String rdOpsPerWriteTx) {
      this.rdOpsPerWriteTx = rdOpsPerWriteTx;
   }

   @ManagedOperation
   public void setRdOpsPerReadTx(String rdOpsPerReadTx) {
      this.rdOpsPerReadTx = rdOpsPerReadTx;
   }

   public void setPerThreadSimulTime(long perThreadSimulTime){
      this.perThreadSimulTime = perThreadSimulTime;
   }

   @ManagedOperation
   public void setNumberOfKeys(int numberOfKeys) {
      this.numberOfKeys = numberOfKeys;
      if (stressor != null) {
         stressor.setNumberOfKeys(numberOfKeys);
      }
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
   public double getExpectedWritePercentage() {
      return writeTransactionPercentage / 100.0;
   }

   @ManagedAttribute
   public String getWrtOpsPerWriteTx() {
      return wrtOpsPerWriteTx;
   }

   @ManagedAttribute
   public String getRdOpsPerWriteTx() {
      return rdOpsPerWriteTx;
   }

   @ManagedAttribute
   public String getRdOpsPerReadTx() {
      return rdOpsPerReadTx;
   }

   @ManagedAttribute
   public int getNumberOfActiveThreads() {
      return numOfThreads;
   }

   @ManagedAttribute
   public int getNumberOfKeys() {
      return numberOfKeys;
   }
}
