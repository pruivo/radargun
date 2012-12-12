package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.jmx.JmxRegistration;
import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedAttribute;
import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.utils.Utils;
import org.radargun.workloads.AbstractTransactionWorkload;
import org.radargun.workloads.KeyGenerator;
import org.radargun.workloads.KeyGeneratorFactory;
import org.radargun.workloads.TransactionWorkloadFactory;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.radargun.utils.Utils.convertNanosToMillis;
import static org.radargun.utils.Utils.getMillisDurationString;

/**
 * On multiple threads executes put and get operations against the CacheWrapper, and returns the result as an Map.
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
@MBean(objectName = "Benchmark", description = "Executes a defined workload over a cache")
public abstract class AbstractBenchmarkStressor<V extends AbstractTransactionWorkload, K extends KeyGeneratorFactory,T extends TransactionWorkloadFactory<V>>
      extends AbstractCacheWrapperStressor {

   protected final K keyGeneratorFactory;
   protected final T transactionWorkload;
   private final Timer stopBenchmarkTimer = new Timer("stop-benchmark-timer");
   private final AtomicBoolean running = new AtomicBoolean(true);
   private final List<Stresser> stresserList = new LinkedList<Stresser>();
   protected Log log = LogFactory.getLog(getClass());
   protected CacheWrapper cacheWrapper;
   private long startTime;
   private volatile CountDownLatch startPoint;
   private int nodeIndex = 0;
   //indicates that the coordinator execute or not txs -- PEDRO
   private boolean coordinatorParticipation = true;
   //simulation time (default: 30 seconds)
   private long simulationTime = 30L;

   protected AbstractBenchmarkStressor(KeyGeneratorFactory keyGeneratorFactory) {
      this.keyGeneratorFactory = keyGeneratorFactory == null ? createDefaultKeyGeneratorFactory() :
            cast(keyGeneratorFactory);
      transactionWorkload = createTransactionWorkloadFactory();
      JmxRegistration.getInstance().processStage(this);
   }

   public AbstractBenchmarkStressor() {
      this(null);
   }

   @Override
   public final Map<String, String> stress(CacheWrapper wrapper) {
      this.cacheWrapper = wrapper;
      keyGeneratorFactory.calculate();
      transactionWorkload.calculate();

      startTime = System.currentTimeMillis();
      log.info("Executing: " + this.toString());

      try {
         executeOperations();
      } catch (Exception e) {
         log.warn("exception when stressing the cache wrapper", e);
         throw new RuntimeException(e);
      }
      return processResults();
   }

   @Override
   public final void destroy() throws Exception {
      cacheWrapper.empty();
      cacheWrapper = null;
   }

   @ManagedOperation
   public final void stopBenchmark() {
      stopBenchmarkTimer.cancel();
      running.set(false);
   }

   @Override
   public final String toString() {
      return "keyGeneratorFactory=" + keyGeneratorFactory +
            ", transactionWorkload=" + transactionWorkload +
            ", coordinatorParticipation=" + coordinatorParticipation +
            ", simulationTime=" + simulationTime +
            ", cacheWrapper=" + cacheWrapper.getInfo() +
            "}";
   }

   public final void setNodeIndex(int nodeIndex) {
      this.nodeIndex = nodeIndex;
   }

   public final void setNumberOfNodes(int numberOfNodes) {
      keyGeneratorFactory.setNumberOfNodes(numberOfNodes);
   }

   //NOTE this time is in seconds!
   public final void setSimulationTime(long simulationTime) {
      this.simulationTime = simulationTime;
   }

   @ManagedAttribute
   public final boolean isNoContention() {
      return keyGeneratorFactory.isNoContention();
   }

   @ManagedOperation
   public final void setNoContention(boolean noContention) {
      keyGeneratorFactory.setNoContention(noContention);
   }

   public final void setBucketPrefix(String bucketPrefix) {
      keyGeneratorFactory.setBucketPrefix(bucketPrefix);
   }

   @ManagedAttribute
   public final int getNumberOfKeys() {
      return keyGeneratorFactory.getNumberOfKeys();
   }

   @ManagedOperation
   public final void setNumberOfKeys(int numberOfKeys) {
      keyGeneratorFactory.setNumberOfKeys(numberOfKeys);
   }

   @ManagedAttribute
   public final int getSizeOfValue() {
      return keyGeneratorFactory.getValueSize();
   }

   @ManagedOperation
   public final void setSizeOfValue(int sizeOfValue) {
      keyGeneratorFactory.setValueSize(sizeOfValue);
   }

   @ManagedAttribute
   public final int getNumberOfThreads() {
      return keyGeneratorFactory.getNumberOfThreads();
   }

   @ManagedOperation
   public final void setNumberOfThreads(int numOfThreads) {
      keyGeneratorFactory.setNumberOfThreads(numOfThreads);
   }

   public final void setCoordinatorParticipation(boolean coordinatorParticipation) {
      this.coordinatorParticipation = coordinatorParticipation;
   }

   @ManagedOperation
   public final void changeKeysWorkload() {
      keyGeneratorFactory.calculate();
   }

   @ManagedOperation
   public final void changeTransactionWorkload() {
      transactionWorkload.calculate();
   }

   @ManagedAttribute
   public final int getLocalityProbability() {
      return keyGeneratorFactory.getLocalityProbability();
   }

   @ManagedOperation
   public final void setLocalityProbability(int localityProbability) {
      keyGeneratorFactory.setLocalityProbability(localityProbability);
   }

   @ManagedOperation
   public final void setStdDev(double stdDev) {
      keyGeneratorFactory.setStdDev(stdDev);
   }

   protected final void begin(AbstractTransactionWorkload txWorkload) {
      cacheWrapper.startTransaction();
      txWorkload.setStartExecutionTimestamp();
   }

   protected final void commit(AbstractTransactionWorkload txWorkload) {
      try {
         cacheWrapper.endTransaction(txWorkload.isExecutionOK());
         txWorkload.setEndCommitTimestamp();
      } catch (Throwable throwable) {
         txWorkload.markCommitFailed();
         txWorkload.setEndCommitTimestamp();
         try {
            cacheWrapper.endTransaction(false);
         } catch (Throwable throwable1) {
            //ignore
         }
      }
   }

   protected abstract K createDefaultKeyGeneratorFactory();

   protected abstract K cast(KeyGeneratorFactory keyGeneratorFactory);

   protected abstract T createTransactionWorkloadFactory();

   protected abstract void executeTransaction(V txWorkload, KeyGenerator keyGenerator);

   protected abstract V chooseNextTransaction(Random random);

   private Map<String, String> processResults() {
      //total duration
      long totalDuration = 0;

      long commitFailedReadOnlyTxDuration = 0;
      int numberOfCommitFailedReadOnlyTx = 0;

      long commitFailedWriteTxDuration = 0;
      int numberOfCommitFailedWriteTx = 0;

      long execFailedReadOnlyTxDuration = 0;
      int numberOfExecFailedReadOnlyTx = 0;


      long execFailedWriteTxDuration = 0;
      int numberOfExecFailedWriteTx = 0;

      long readOnlyTxDuration = 0;
      int numberOfReadOnlyTx = 0;

      long writeTxDuration = 0;
      int numberOfWriteTx = 0;

      long readOnlyTxCommitSuccessDuration = 0;
      long writeTxCommitSuccessDuration = 0;

      long readOnlyTxCommitFailDuration = 0;
      long writeTxCommitFailDuration = 0;

      long readOnlyTxRollbackDuration = 0;
      long writeTxRollbackDuration = 0;

      for (Stresser stresser : stresserList) {
         totalDuration += stresser.delta;

         commitFailedReadOnlyTxDuration += stresser.commitFailedReadOnlyTxDuration;
         numberOfCommitFailedReadOnlyTx += stresser.numberOfCommitFailedReadOnlyTx;

         commitFailedWriteTxDuration += stresser.commitFailedWriteTxDuration;
         numberOfCommitFailedWriteTx += stresser.numberOfCommitFailedWriteTx;

         execFailedReadOnlyTxDuration += stresser.execFailedReadOnlyTxDuration;
         numberOfExecFailedReadOnlyTx += stresser.numberOfExecFailedReadOnlyTx;

         execFailedWriteTxDuration += stresser.execFailedWriteTxDuration;
         numberOfExecFailedWriteTx += stresser.numberOfExecFailedWriteTx;

         readOnlyTxDuration += stresser.readOnlyTxDuration;
         numberOfReadOnlyTx += stresser.numberOfReadOnlyTx;

         writeTxDuration += stresser.writeTxDuration;
         numberOfWriteTx += stresser.numberOfWriteTx;

         readOnlyTxCommitSuccessDuration += stresser.readOnlyTxCommitSuccessDuration;
         writeTxCommitSuccessDuration += stresser.writeTxCommitSuccessDuration;

         readOnlyTxCommitFailDuration += stresser.readOnlyTxCommitFailDuration;
         writeTxCommitFailDuration += stresser.writeTxCommitFailDuration;

         readOnlyTxRollbackDuration += stresser.readOnlyTxTollbackDuration;
         writeTxRollbackDuration += stresser.writeTxTollbackDuration;
      }

      Map<String, String> results = new LinkedHashMap<String, String>();
      int numOfThreads = stresserList.size();

      results.put("DURATION(msec)", str(convertNanosToMillis(totalDuration) / numOfThreads));
      results.put("TX_PER_SEC", str(calculateTxPerSec(numberOfReadOnlyTx + numberOfWriteTx,convertNanosToMillis(totalDuration), numOfThreads)));
      results.put("RO_TX_PER_SEC", str(calculateTxPerSec(numberOfReadOnlyTx, convertNanosToMillis(totalDuration), numOfThreads)));
      results.put("WRT_TX_SEC", str(calculateTxPerSec(numberOfWriteTx, convertNanosToMillis(totalDuration), numOfThreads)));

      results.put("WRT_TX_DUR(msec)", str(convertNanosToMillis(writeTxDuration / numOfThreads)));
      results.put("RO_TX_DUR(msec)", str(convertNanosToMillis(readOnlyTxDuration / numOfThreads)));

      if(numberOfReadOnlyTx != 0) {
         results.put("AVG_RO_OK_TX_DUR(msec)",str(convertNanosToMillis(readOnlyTxDuration / numOfThreads) / numberOfReadOnlyTx));
      } else {
         results.put("AVG_RO_OK_TX_DUR(msec)",str(0));
      }

      if(numberOfExecFailedReadOnlyTx != 0) {
         results.put("AVG_RO_EXEC_ERR_TX_DUR(msec)",str(convertNanosToMillis(execFailedReadOnlyTxDuration / numOfThreads) / numberOfExecFailedReadOnlyTx));
      } else {
         results.put("AVG_RO_EXEC_ERR_TX_DUR(msec)",str(0));
      }

      if(numberOfCommitFailedReadOnlyTx != 0) {
         results.put("AVG_RO_COMMIT_ERR_TX_DUR(msec)",str(convertNanosToMillis(commitFailedReadOnlyTxDuration / numOfThreads) / numberOfCommitFailedReadOnlyTx));
      } else {
         results.put("AVG_RO_COMMIT_ERR_TX_DUR(msec)",str(0));
      }

      if(numberOfWriteTx != 0) {
         results.put("AVG_WRT_OK_TX_DUR(msec)",str(convertNanosToMillis(writeTxDuration / numOfThreads) / numberOfWriteTx));
      } else {
         results.put("AVG_WRT_OK_TX_DUR(msec)",str(0));
      }

      if(numberOfExecFailedWriteTx != 0) {
         results.put("AVG_WRT_EXEC_ERR_TX_DUR(msec)",str(convertNanosToMillis(execFailedWriteTxDuration / numOfThreads) / numberOfExecFailedWriteTx));
      } else {
         results.put("AVG_WRT_EXEC_ERR_TX_DUR(msec)",str(0));
      }

      if(numberOfCommitFailedWriteTx != 0) {
         results.put("AVG_WRT_COMMIT_ERR_TX_DUR(sec)",str(convertNanosToMillis(commitFailedWriteTxDuration / numOfThreads) / numberOfCommitFailedWriteTx));
      } else {
         results.put("AVG_WRT_COMMIT_ERR_TX_DUR(sec)",str(0));
      }

      if(numberOfReadOnlyTx != 0) {
         results.put("AVG_OK_RO_COMMIT_DUR(msec)",str(convertNanosToMillis(readOnlyTxCommitSuccessDuration / numOfThreads) / numberOfReadOnlyTx));
      } else {
         results.put("AVG_OK_RO_COMMIT_DUR(msec)",str(0));
      }

      if(numberOfCommitFailedReadOnlyTx != 0) {
         results.put("AVG_ERR_RO_COMMIT_DUR(msec)",str(convertNanosToMillis(readOnlyTxCommitFailDuration / numOfThreads) / numberOfCommitFailedReadOnlyTx));
      } else {
         results.put("AVG_ERR_RO_COMMIT_DUR(msec)",str(0));
      }

      if(numberOfExecFailedReadOnlyTx != 0) {
         results.put("AVG_RO_ROLLBACK_DUR(msec)",str(convertNanosToMillis(readOnlyTxRollbackDuration / numOfThreads) / numberOfExecFailedReadOnlyTx));
      } else {
         results.put("AVG_RO_ROLLBACK_DUR(msec)",str(0));
      }

      if(numberOfWriteTx != 0) {
         results.put("AVG_OK_WRT_COMMIT_DUR(msec)",str(convertNanosToMillis(writeTxCommitSuccessDuration / numOfThreads) / numberOfWriteTx));
      } else {
         results.put("AVG_OK_WRT_COMMIT_DUR(msec)",str(0));
      }

      if(numberOfCommitFailedWriteTx != 0) {
         results.put("AVG_ERR_WRT_COMMIT_DUR(msec)",str(convertNanosToMillis(writeTxCommitFailDuration / numOfThreads) / numberOfCommitFailedWriteTx));
      } else {
         results.put("AVG_ERR_WRT_COMMIT_DUR(msec)",str(0));
      }

      if(numberOfExecFailedWriteTx != 0) {
         results.put("AVG_WRT_ROLLBACK_DUR(msec)",str(convertNanosToMillis(writeTxRollbackDuration / numOfThreads) / numberOfExecFailedWriteTx));
      } else {
         results.put("AVG_WRT_ROLLBACK_DUR(msec)",str(0));
      }

      results.put("RO_TX_COUNT", str(numberOfReadOnlyTx));
      results.put("RO_EXEC_ERR_TX_COUNT", str(numberOfExecFailedReadOnlyTx));
      results.put("RO_COMMIT_ERR_TX_COUNT", str(numberOfCommitFailedReadOnlyTx));
      results.put("WRT_TX_COUNT", str(numberOfWriteTx));
      results.put("WRT_EXEC_ERR_TX_COUNT", str(numberOfExecFailedWriteTx));
      results.put("WRT_COMMIT_ERR_TX_COUNT", str(numberOfCommitFailedWriteTx));

      int totalFailedTx = numberOfExecFailedReadOnlyTx + numberOfCommitFailedReadOnlyTx + numberOfExecFailedWriteTx +
            numberOfCommitFailedWriteTx;

      results.putAll(cacheWrapper.getAdditionalStats());

      log.info("Finished generating report. Nr of failed transactions on this node is: " + totalFailedTx +
                     ". Test duration is: " + getMillisDurationString(System.currentTimeMillis() - startTime));

      return results;
   }

   private double calculateTxPerSec(int txCount, double txDuration, int numOfThreads) {
      if (txDuration <= 0) {
         return 0;
      }
      return txCount / ((txDuration / numOfThreads) / 1000.0);
   }

   private void executeOperations() throws Exception {
      startPoint = new CountDownLatch(1);

      for (int threadIndex = 0; threadIndex < keyGeneratorFactory.getNumberOfThreads(); threadIndex++) {
         Stresser stresser = new Stresser(threadIndex);
         stresserList.add(stresser);

         try{
            stresser.start();
         } catch (Throwable t){
            log.warn("Error starting all the stressers", t);
         }
      }

      log.info("Cache private class Stresser extends Thread { wrapper info is: " + cacheWrapper.getInfo());
      startPoint.countDown();
      stopBenchmarkTimer.schedule(new TimerTask() {
         @Override
         public void run() {
            finishBenchmark();
         }
      }, simulationTime * 1000);
      for (Stresser stresser : stresserList) {
         stresser.join();
         log.info("stresser[" + stresser.getName() + "] finished");
      }
      log.info("All stressers have finished their execution");
   }

   private String str(Object o) {
      return String.valueOf(o);
   }

   private void finishBenchmark() {
      running.set(false);
   }

   private class Stresser extends Thread {

      private final Random random;
      private KeyGenerator keyGenerator;
      private long delta = 0;
      private long startTime = 0;
      //execution successful and commit successful
      private long readOnlyTxCommitSuccessDuration;
      private long writeTxCommitSuccessDuration;
      //execution successful but the commit fails
      private long readOnlyTxCommitFailDuration;
      private long writeTxCommitFailDuration;
      //execution failed
      private long readOnlyTxTollbackDuration;
      private long writeTxTollbackDuration;
      //exec: OK, commit: ERR
      private long commitFailedReadOnlyTxDuration;
      private int numberOfCommitFailedReadOnlyTx;
      //exec: OK, commit: ERR
      private long commitFailedWriteTxDuration;
      private int numberOfCommitFailedWriteTx;
      //exec: ERR
      private long execFailedReadOnlyTxDuration;
      private int numberOfExecFailedReadOnlyTx;
      //exec: ERR
      private long execFailedWriteTxDuration;
      private int numberOfExecFailedWriteTx;
      //exec: OK, commit: OK
      private long readOnlyTxDuration = 0;
      private int numberOfReadOnlyTx = 0;
      //exec: OK, commit: OK
      private long writeTxDuration = 0;
      private int numberOfWriteTx = 0;

      public Stresser(int threadIndex) {
         super("Stresser-" + threadIndex);
         this.random = new Random(System.nanoTime());
         this.keyGenerator = keyGeneratorFactory.createKeyGenerator(nodeIndex, threadIndex);
      }

      @Override
      public void run() {
         int i = 0;

         try {
            startPoint.await();
            log.info("Starting thread: " + getName());
         } catch (InterruptedException e) {
            log.warn(e);
         }

         startTime = System.nanoTime();
         if(coordinatorParticipation || !cacheWrapper.isTheMaster()) {

            while(running.get()){

               V txWorkload = chooseNextTransaction(random);

               log.trace("*** [" + getName() + "] new transaction: " + i + "***");

               executeTransaction(txWorkload, keyGenerator);

               log.trace("*** [" + getName() + "] end transaction: " + i++ + "***");

               //update stats
               if(txWorkload.isExecutionOK()) {
                  if (txWorkload.isCommitOK()) {
                     if (txWorkload.isReadOnly()) {
                        readOnlyTxCommitSuccessDuration += txWorkload.getCommitDuration();
                        readOnlyTxDuration += txWorkload.getExecutionDuration();
                        numberOfReadOnlyTx++;
                     } else {
                        writeTxCommitSuccessDuration += txWorkload.getCommitDuration();
                        writeTxDuration += txWorkload.getExecutionDuration();
                        numberOfWriteTx++;
                     }
                  } else {
                     if (txWorkload.isReadOnly()) {
                        readOnlyTxCommitFailDuration += txWorkload.getCommitDuration();
                        commitFailedReadOnlyTxDuration += txWorkload.getExecutionDuration();
                        numberOfCommitFailedReadOnlyTx++;
                     } else {
                        writeTxCommitFailDuration += txWorkload.getCommitDuration();
                        commitFailedWriteTxDuration += txWorkload.getExecutionDuration();
                        numberOfCommitFailedWriteTx++;
                     }
                  }
               } else {
                  //it is a rollback
                  if (txWorkload.isReadOnly()) {
                     readOnlyTxTollbackDuration += txWorkload.getCommitDuration();
                     execFailedReadOnlyTxDuration += txWorkload.getExecutionDuration();
                     numberOfExecFailedReadOnlyTx++;
                  } else {
                     writeTxTollbackDuration += txWorkload.getCommitDuration();
                     execFailedWriteTxDuration += txWorkload.getExecutionDuration();
                     numberOfExecFailedWriteTx++;
                  }
               }

               this.delta = System.nanoTime() - startTime;
            }
         } else {
            long sleepTime = (long) Utils.convertNanosToMillis(simulationTime); //nano to millis
            log.info("I am a coordinator and I wouldn't execute transactions. sleep for " + sleepTime + "(ms)");
            try {
               Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
               log.info("interrupted exception when sleeping (I'm coordinator and I don't execute transactions)");
            }
         }
      }
   }
}

