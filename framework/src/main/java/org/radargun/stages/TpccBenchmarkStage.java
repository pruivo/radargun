package org.radargun.stages;

import static java.lang.Double.parseDouble;
import static org.radargun.utils.Utils.numberFormat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.TpccStressor;

/**
 * Simulate the activities found in complex OLTP application environments.
 * Execute the TPC-C Benchmark.
 * <pre>
 * Params:
 *       - numOfThreads : the number of stressor threads that will work on each slave.
 *       - perThreadSimulTime : total time (in seconds) of simulation for each stressor thread.
 *       - arrivalRate : if the value is greater than 0.0, the "open system" mode is active and the parameter represents the arrival rate (in transactions per second) of a job (a transaction to be executed) to the system; otherwise the "closed system" mode is active: this means that each thread generates and executes a new transaction in an iteration as soon as it has completed the previous iteration.
 *       - paymentWeight : percentage of Payment transactions.
 *       - orderStatusWeight : percentage of Order Status transactions.
 * </pre>
 *
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 */
public class TpccBenchmarkStage extends AbstractDistStage {
   
   private static final String SIZE_INFO = "SIZE_INFO";

   /**
    * the number of threads that will work on this slave
    */
   private int numOfThreads = 10;
   
   /**
    * total time (in seconds) of simulation for each stressor thread
    */
   private long perThreadSimulTime = 180L;
   
   /**
    * average arrival rate of the transactions to the system
    */
   private double arrivalRate = 0.0D;
   
   /**
    * percentage of Payment transactions
    */
   private double paymentWeight = 45.0D;
   
   /**
    * percentage of Order Status transactions
    */
   private double orderStatusWeight = 5.0D;

   /**
    * if true, each node will pick a warehouse and all transactions will work over that warehouse. The warehouses are
    * picked by order, i.e., slave 0 gets warehouse 1,N+1, 2N+1,[...]; ... slave N-1 gets warehouse N, 2N, [...].
    */
   private boolean accessSameWarehouse = false;

   private CacheWrapper cacheWrapper;

   public DistStageAck executeOnSlave() {
      DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
      this.cacheWrapper = slaveState.getCacheWrapper();
      if (cacheWrapper == null) {
         log.info("Not running test on this slave as the wrapper hasn't been configured.");
         return result;
      }

      log.info("Starting TpccBenchmarkStage: " + this.toString());

      TpccStressor tpccStressor = new TpccStressor();
      tpccStressor.setNodeIndex(getSlaveIndex());
      tpccStressor.setNumSlaves(getActiveSlaveCount());
      tpccStressor.setNumOfThreads(this.numOfThreads);
      tpccStressor.setPerThreadSimulTime(this.perThreadSimulTime);
      tpccStressor.setArrivalRate(this.arrivalRate);
      tpccStressor.setPaymentWeight(this.paymentWeight);
      tpccStressor.setOrderStatusWeight(this.orderStatusWeight);
      tpccStressor.setAccessSameWarehouse(accessSameWarehouse);

      try {
         Map<String, String> results = tpccStressor.stress(cacheWrapper);
         String sizeInfo = "size info: " + cacheWrapper.getInfo() +
               ", clusterSize:" + super.getActiveSlaveCount() +
               ", nodeIndex:" + super.getSlaveIndex() +
               ", cacheSize: " + cacheWrapper.getCacheSize();

         log.info(sizeInfo);
         results.put(SIZE_INFO, sizeInfo);
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
            log.info("Received " +  benchResult.remove(SIZE_INFO));
         } else {
            log.trace("No report received from slave: " + ack.getSlaveIndex());
         }
      }
      return success;
   }

   public void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
   }
   
   public void setPerThreadSimulTime(long perThreadSimulTime) {
      this.perThreadSimulTime = perThreadSimulTime;
   }

   public void setArrivalRate(double arrivalRate) {
      this.arrivalRate = arrivalRate;
   }

   public void setPaymentWeight(double paymentWeight) {
      this.paymentWeight = paymentWeight;
   }

   public void setOrderStatusWeight(double orderStatusWeight) {
      this.orderStatusWeight = orderStatusWeight;
   }

   public void setAccessSameWarehouse(boolean accessSameWarehouse) {
      this.accessSameWarehouse = accessSameWarehouse;
   }

   @Override
   public String toString() {
      return "TpccBenchmarkStage {" +
            "numOfThreads=" + numOfThreads +
            ", perThreadSimulTime=" + perThreadSimulTime +
            ", arrivalRate=" + arrivalRate +
            ", paymentWeight=" + paymentWeight +
            ", orderStatusWeight=" + orderStatusWeight +
            ", accessSameWarehouse=" + accessSameWarehouse +
            ", cacheWrapper=" + cacheWrapper +
            ", " + super.toString();
   }

}
