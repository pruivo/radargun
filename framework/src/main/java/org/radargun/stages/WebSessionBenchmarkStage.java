package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedAttribute;
import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.keygen2.KeyGeneratorFactory;
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

   private String writeTxWorkload = "50;50";

   private String readTxWorkload = "100";   
   
   private String bucketPrefix = null;

   //simulation time (in seconds)
   private long perThreadSimulTime;

   //allows execution without contention
   private boolean noContention = false;

   private CacheWrapper cacheWrapper;   
   private boolean reportNanos = false;   

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

      KeyGeneratorFactory factory = (KeyGeneratorFactory) slaveState.get("key_gen_factory");
      factory.setBucketPrefix(bucketPrefix);
      factory.setValueSize(sizeOfValue);
      factory.setNoContention(noContention);
      factory.setNumberOfKeys(numberOfKeys);
      factory.setNumberOfNodes(getActiveSlaveCount());
      factory.setNumberOfThreads(numOfThreads);      

      log.info("Starting WebSessionBenchmarkStage: " + this.toString());

      PutGetStressor stressor = new PutGetStressor(factory);
      stressor.setNodeIndex(getSlaveIndex());      
      stressor.setSimulationTime(perThreadSimulTime);
      stressor.setWriteTransactionPercentage(writeTransactionPercentage);
      stressor.setCoordinatorParticipation(coordinatorParticipation);
      stressor.setWriteTxWorkload(writeTxWorkload);
      stressor.setReadTxWorkload(readTxWorkload);

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

   private boolean validate(String string) {
      if (string == null || string.equals("")) {
         return false;
      }
      String[] split = string.split(":");
      if (split.length == 0 || split.length > 2) {
         return false;
      }

      for (String s : split) {
         try {
            Integer.parseInt(s);
         } catch (NumberFormatException nfe) {
            log.warn("Error validating " + string + ". " + nfe.getMessage());
            return false;
         }
      }
      return true;
   }

   @Override
   public String toString() {
      return "WebSessionBenchmarkStage {" +            
            "numberOfKeys=" + numberOfKeys +
            ", sizeOfValue=" + sizeOfValue +
            ", writeTransactionPercentage=" + writeTransactionPercentage +
            ", numOfThreads=" + numOfThreads +
            ", reportNanos=" + reportNanos +
            ", coordinatorParticipation=" + coordinatorParticipation +
            ", writeTxWorkload=" + writeTxWorkload +
            ", readTxWorkload=" + readTxWorkload +            
            ", perThreadSimulTime=" + perThreadSimulTime +
            ", noContention=" + noContention +
            ", cacheWrapper=" + cacheWrapper +
            ", " + super.toString();
   }

   @ManagedOperation
   public void setWriteTxWorkload(String writeTxWorkload) {
      this.writeTxWorkload = writeTxWorkload;
   }

   @ManagedOperation
   public void setReadTxWorkload(String readTxWorkload) {
      this.readTxWorkload = readTxWorkload;
   }

   public void setPerThreadSimulTime(long perThreadSimulTime){
      this.perThreadSimulTime = perThreadSimulTime;
   }

   @ManagedOperation
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

   public void setCoordinatorParticipation(boolean coordinatorParticipation) {
      this.coordinatorParticipation = coordinatorParticipation;
   }

   public void setNoContention(boolean noContention) {
      this.noContention = noContention;
   }

   @ManagedOperation
   public void setWriteTransactionPercentage(int writeTransactionPercentage) {
      this.writeTransactionPercentage = writeTransactionPercentage;
   }   

   @ManagedAttribute
   public double getExpectedWritePercentage() {
      return writeTransactionPercentage / 100.0;
   }

   @ManagedAttribute
   public String getWriteTxWorkload() {
      return writeTxWorkload;
   }

   @ManagedAttribute
   public String getReadTxWorkload() {
      return readTxWorkload;
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
