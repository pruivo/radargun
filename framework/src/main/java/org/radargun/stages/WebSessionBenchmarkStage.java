package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
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
public class WebSessionBenchmarkStage extends AbstractDistStage {

   private int opsCountStatusLog = 5000;

   public static final String SESSION_PREFIX = "SESSION";

   /**
    * total number of request to be made against this session: reads + writes
    */
   private int numberOfRequests = 50000;

   /**
    * for each session there will be created fixed number of attributes. On those attributes all the GETs and PUTs are
    * performed (for PUT is overwrite)
    */
   private int numberOfAttributes = 10000;

   /**
    * Each attribute will be a byte[] of this size
    */
   private int sizeOfAnAttribute = 1000;

   /**
    * Out of the total number of request, this define the frequency of writes (percentage)
    */
   private int writePercentage = 20;


   /**
    * the number of threads that will work on this slave
    */
   private int numOfThreads = 10;

   private boolean reportNanos = false;

    //indicates that the coordinator executes transactions or not
    private boolean coordinatorParticipation = true;

   private CacheWrapper cacheWrapper;


    private int total_num_of_slaves;//needed to compute the write percentage given for the master given the number of per-cache operation
    private int lowerBoundOp = 100;     //needed to compute the uniform distribution of operations per transaction
    private int upperBoundOp = 100;     // as above
    private long perThreadSimulTime;


   public DistStageAck executeOnSlave() {
      DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
      this.cacheWrapper = slaveState.getCacheWrapper();
      if (cacheWrapper == null) {
         log.info("Not running test on this slave as the wrapper hasn't been configured.");
         return result;
      }

      log.info("Starting WebSessionBenchmarkStage: " + this.toString());
      //Added by Diego
      PutGetStressor putGetStressor = new PutGetStressor(cacheWrapper.isPrimary(), total_num_of_slaves,this.lowerBoundOp,this.upperBoundOp,this.perThreadSimulTime);
      putGetStressor.setBucketPrefix(getSlaveIndex() + "");
      putGetStressor.setNumberOfAttributes(numberOfAttributes);
      putGetStressor.setNumberOfRequests(numberOfRequests);
      putGetStressor.setNumOfThreads(numOfThreads);
      putGetStressor.setOpsCountStatusLog(opsCountStatusLog);
      putGetStressor.setSizeOfAnAttribute(sizeOfAnAttribute);
      putGetStressor.setWritePercentage(writePercentage);
       putGetStressor.setCoordinatorParticipation(coordinatorParticipation);

       //ATTENTION! THE CACHEWRAPPER IS A PARAMETER FOR THE STRESS METHOD SO IT'S NOT SET UPON THE CALL TO THE
       //STRESSOR CONSTRUCTOR!

      try {
         Map<String, String> results = putGetStressor.stress(cacheWrapper);
         result.setPayload(results);
         return result;
      } catch (Exception e) {
         log.warn("Exception while initializing the test", e);
         result.setError(true);
         result.setRemoteException(e);
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
            Object reqPerSes = benchResult.get("REQ_PER_SEC");
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

   public void setNumberOfRequests(int numberOfRequests) {
      this.numberOfRequests = numberOfRequests;
   }

   public void setNumberOfAttributes(int numberOfAttributes) {
      this.numberOfAttributes = numberOfAttributes;
   }

   public void setSizeOfAnAttribute(int sizeOfAnAttribute) {
      this.sizeOfAnAttribute = sizeOfAnAttribute;
   }

   public void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
   }

   public void setReportNanos(boolean reportNanos) {
      this.reportNanos = reportNanos;
   }

   public void setWritePercentage(int writePercentage) {
      this.writePercentage = writePercentage;
   }

   public void setOpsCountStatusLog(int opsCountStatusLog) {
      this.opsCountStatusLog = opsCountStatusLog;
   }

    public void setCoordinatorParticipation(boolean coordinatorParticipation) {
      this.coordinatorParticipation = coordinatorParticipation;
   }

   @Override
   public String toString() {
      return "WebSessionBenchmarkStage {" +
            "opsCountStatusLog=" + opsCountStatusLog +
            ", numberOfRequests=" + numberOfRequests +
            ", numberOfAttributes=" + numberOfAttributes +
            ", sizeOfAnAttribute=" + sizeOfAnAttribute +
            ", writePercentage=" + writePercentage +
            ", numOfThreads=" + numOfThreads +
            ", reportNanos=" + reportNanos +
            ", cacheWrapper=" + cacheWrapper +
            ", " + super.toString();
   }



    public void setNumSlaves(int no){

        this.total_num_of_slaves=no;
    }


    public void setLowerBoundOp(int lb){

        this.lowerBoundOp=lb;
    }


    public void setUpperBoundOp(int ub){

        this.upperBoundOp=ub;
    }


    public void setPerThreadSimulTime(long l){

        this.perThreadSimulTime=l;

    }


    public long getPerThreadSimulTime(){

        return this.perThreadSimulTime;
    }
}
