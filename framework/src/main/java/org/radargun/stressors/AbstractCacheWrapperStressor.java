package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.utils.StatSampler;
import org.radargun.utils.Utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.radargun.utils.Utils.convertNanosToMillis;

/**
 * On multiple threads executes put and get operations against the CacheWrapper, and returns the result as an Map.
 * @author Pedro Ruivo
 * @since 1.1
 */
public abstract class AbstractCacheWrapperStressor implements CacheWrapperStressor {

   private static Log log = LogFactory.getLog(AbstractCacheWrapperStressor.class);

   private CacheWrapper cacheWrapper;
   private long startTime;
   private volatile CountDownLatch startPoint;

   //this slave index
   private int slaveIdx = 0;

   //the number of nodes
   private int numberOfNodes = 1;

   //the number of threads that will work on this cache wrapper.
   private int numOfThreads = 1;

   //indicates that the coordinator execute or not txs -- PEDRO
   private boolean coordinatorParticipation = true;

   //simulation time (in nanoseconds) (default: 30 seconds) (Note: it converts from seconds to nanoseconds in the setter)
   private long simulationTime = 30000000000L;

   //collect samples during runtime about the memory and cpu
   private StatSampler statSampler;

   //the sample interval
   private long statsSamplingInterval = 0;   

   public Map<String, String> stress(CacheWrapper wrapper) {      
      this.cacheWrapper = wrapper;
      initBeforeStress();

      startTime = System.currentTimeMillis();
      log.info("Executing: " + this.toString());

      List<Stresser> stressers;
      try {
         log.warn("Resetting statistics before the PutGetStressors start executing");
         wrapper.resetAdditionalStats();
         if(this.statsSamplingInterval > 0) {
            this.statSampler = new StatSampler(this.statsSamplingInterval);
         }
         stressers = executeOperations();
      } catch (Exception e) {
         log.warn("exception when stressing the cache wrapper", e);
         throw new RuntimeException(e);
      }
      return processResults(stressers);
   }

   public void destroy() throws Exception {
      cacheWrapper.empty();
      cacheWrapper = null;
   }

   private Map<String, String> processResults(List<Stresser> stressers) {
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

      for (Stresser stresser : stressers) {
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

      results.put("DURATION(msec)", str(convertNanosToMillis(totalDuration) / numOfThreads));
      results.put("TX_PER_SEC", str(calculateTxPerSec(numberOfReadOnlyTx + numberOfWriteTx,convertNanosToMillis(totalDuration))));
      results.put("RO_TX_PER_SEC", str(calculateTxPerSec(numberOfReadOnlyTx, convertNanosToMillis(totalDuration))));
      results.put("WRT_TX_SEC", str(calculateTxPerSec(numberOfWriteTx, convertNanosToMillis(totalDuration))));

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
      afterResults(stressers, cacheWrapper, results);

      log.info("Finished generating report. Nr of failed transactions on this node is: " + totalFailedTx +
                     ". Test duration is: " + Utils.getDurationString(System.currentTimeMillis() - startTime));

      return results;
   }

   private double calculateTxPerSec(int txCount, double txDuration) {
      if (txDuration <= 0) {
         return 0;
      }
      return txCount / ((txDuration / numOfThreads) / 1000.0);
   }

   private List<Stresser> executeOperations() throws Exception {
      List<Stresser> stresserList = new ArrayList<Stresser>();
      startPoint = new CountDownLatch(1);

      for (int threadIndex = 0; threadIndex < numOfThreads; threadIndex++) {
         Stresser stresser = createStresser(threadIndex);
         stresserList.add(stresser);

         try{
            if (statSampler != null) {
               statSampler.reset();
               statSampler.start();
            }
            stresser.start();
         }
         catch (Throwable t){
            log.warn("Error starting all the stressers", t);
         }
      }

      log.info("Cache private class Stresser extends Thread { wrapper info is: " + cacheWrapper.getInfo());
      startPoint.countDown();
      for (Stresser stresser : stresserList) {
         stresser.join();
         log.info("stresser[" + stresser.getName() + "] finished");
      }
      log.info("All stressers have finished their execution");
      if (statSampler != null) {
         statSampler.cancel();
      }

      afterStresser(stresserList, cacheWrapper);
      saveSamples();

      return stresserList;
   }

   protected abstract Stresser createStresser(int threadIdx);
   protected abstract void afterStresser(List<Stresser> stressers, CacheWrapper cacheWrapper);
   protected abstract void afterResults(List<Stresser> stressers, CacheWrapper cacheWrapper, Map<String, String> results);
   protected abstract void initBeforeStress();

   protected abstract class Stresser extends Thread {

      private int threadIndex;

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
         this.threadIndex = threadIndex;
      }

      @Override
      public void run() {
         int txCounter = 0;

         try {
            startPoint.await();
            log.info("Starting thread: " + getName());
         } catch (InterruptedException e) {
            log.warn(e);
         }

         startTime = System.nanoTime();
         if(coordinatorParticipation || !cacheWrapper.isCoordinator()) {

            while(delta < simulationTime){


               log.trace("*** [" + getName() + "] new transaction: " + txCounter + "***");
               long startTx = System.nanoTime();
               TransactionResult result = executeTransaction(cacheWrapper);
               log.trace("*** [" + getName() + "] end transaction: " + txCounter++ + "***");
               long endTx = System.nanoTime();

               long commitDuration = result.getCommitTime();
               long totalTxDuration = endTx - startTx;

               boolean executionSuccessful = result.isExecutionSuccessful();
               boolean commitSuccessful = result.isCommitSuccessful();
               boolean readOnlyTransaction = result.isReadOnly();

               //update stats
               if(executionSuccessful) {
                  if (commitSuccessful) {
                     if (readOnlyTransaction) {
                        readOnlyTxCommitSuccessDuration += commitDuration;
                        readOnlyTxDuration += totalTxDuration;
                        numberOfReadOnlyTx++;
                     } else {
                        writeTxCommitSuccessDuration += commitDuration;
                        writeTxDuration += totalTxDuration;
                        numberOfWriteTx++;
                     }
                  } else {
                     if (readOnlyTransaction) {
                        readOnlyTxCommitFailDuration += commitDuration;
                        commitFailedReadOnlyTxDuration += totalTxDuration;
                        numberOfCommitFailedReadOnlyTx++;
                     } else {
                        writeTxCommitFailDuration += commitDuration;
                        commitFailedWriteTxDuration += totalTxDuration;
                        numberOfCommitFailedWriteTx++;
                     }
                  }
               } else {
                  //it is a rollback                  
                  if (readOnlyTransaction) {
                     readOnlyTxTollbackDuration += commitDuration;
                     execFailedReadOnlyTxDuration += totalTxDuration;
                     numberOfExecFailedReadOnlyTx++;
                  } else {
                     writeTxTollbackDuration += commitDuration;
                     execFailedWriteTxDuration += totalTxDuration;
                     numberOfExecFailedWriteTx++;
                  }
               }

               this.delta = System.nanoTime() - startTime;
            }
         } else {
            long sleepTime = simulationTime / 1000000; //nano to millis
            log.info("I am a coordinator and I wouldn't execute transactions. sleep for " + sleepTime + "(ms)");
            try {
               Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
               log.info("interrupted exception when sleeping (I'm coordinator and I don't execute transactions)");
            }
         }
      }

      protected abstract TransactionResult executeTransaction(CacheWrapper cacheWrapper);

      protected int getThreadIndex() {
         return threadIndex;
      }
   }

   protected class TransactionResult {
      private final boolean executionSuccessful;
      private final boolean commitSuccessful;
      private final boolean readOnly;
      private final long commitTime;

      protected TransactionResult(boolean executionSuccessful, boolean commitSuccessful, boolean readOnly, long commitTime) {
         this.executionSuccessful = executionSuccessful;
         this.commitSuccessful = commitSuccessful;
         this.readOnly = readOnly;
         this.commitTime = commitTime;
      }

      public boolean isExecutionSuccessful() {
         return executionSuccessful;
      }

      public boolean isCommitSuccessful() {
         return commitSuccessful;
      }

      public boolean isReadOnly() {
         return readOnly;
      }

      public long getCommitTime() {
         return commitTime;
      }
   }

   private String str(Object o) {
      return String.valueOf(o);
   }

   private void saveSamples() {
      if (statSampler == null) {
         return;
      }
      log.info("Saving samples in the file sample-" + slaveIdx);
      File f = new File("sample-" + slaveIdx);
      try {
         BufferedWriter bw = new BufferedWriter(new FileWriter(f));
         List<Long> mem = statSampler.getMemoryUsageHistory();
         List<Double> cpu = statSampler.getCpuUsageHistory();

         int size = Math.min(mem.size(), cpu.size());
         bw.write("#Time (milliseconds)\tCPU(%)\tMemory(bytes)");
         bw.newLine();
         for (int i = 0; i < size; ++i) {
            bw.write((i * statsSamplingInterval) + "\t" + cpu.get(i) + "\t" + mem.get(i));
            bw.newLine();
         }
         bw.flush();
         bw.close();
      } catch (IOException e) {
         log.warn("IOException caught while saving sampling: " + e.getMessage());
      }
   }

   @Override
   public String toString() {
      return "numOfThreads=" + numOfThreads +
            ", coordinatorParticipation=" + coordinatorParticipation +
            ", simulationTime=" + simulationTime +
            ", cacheWrapper=" + cacheWrapper.getInfo() +
            "}";
   }

   /*
   * -----------------------------------------------------------------------------------
   * SETTERS
   * -----------------------------------------------------------------------------------
   */

   public void setSlaveIdx(int slaveIdx) {
      this.slaveIdx = slaveIdx;
   }

   public void setNumberOfNodes(int numberOfNodes) {
      this.numberOfNodes = numberOfNodes;
   }

   //NOTE this time is in seconds!
   public void setSimulationTime(long simulationTime) {
      this.simulationTime = simulationTime * 1000000000;
   }

   public void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
   }

   public void setCoordinatorParticipation(boolean coordinatorParticipation) {
      this.coordinatorParticipation = coordinatorParticipation;
   }

   public void setStatsSamplingInterval(long l){
      this.statsSamplingInterval = l;
   }  
}

