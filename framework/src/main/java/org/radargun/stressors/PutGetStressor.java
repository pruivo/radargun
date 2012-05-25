package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.utils.BucketsKeysTreeSet;
import org.radargun.utils.KeyGenerator;
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
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.radargun.utils.Utils.convertNanosToMillis;

/**
 * On multiple threads executes put and get operations against the CacheWrapper, and returns the result as an Map.
 *
 * @author Mircea.Markus@jboss.com
 * changed by:
 * @author Pedro Ruivo
 */
public class PutGetStressor implements CacheWrapperStressor {

   private static Log log = LogFactory.getLog(PutGetStressor.class);

   private CacheWrapper cacheWrapper;
   //private static Random r = new Random();
   private long startTime;
   private volatile CountDownLatch startPoint;
   private String bucketPrefix = null;
   private int opsCountStatusLog = 5000;
   private int slaveIdx = 1;

   //for each there will be created fixed number of keys. All the GETs and PUTs are performed on these keys only.
   private int numberOfKeys = 1000;

   //Each key will be a byte[] of this size.
   private int sizeOfValue = 1000;

   //Out of the total number of operations, this defines the frequency of writes (percentage).
   private int writeOperationPercentage = 20;

   //The percentage of write transactions generated
   private int writeTransactionPercentage = 100;

   //the number of threads that will work on this cache wrapper.
   private int numOfThreads = 1;

   //indicates that the coordinator execute or not txs -- PEDRO
   private boolean coordinatorParticipation = true;

   //the minimum number of operations per transaction
   private int lowerBoundOp = 10;

   //hte maximum number of operations per transaction
   private int upperBoundOp = 10;

   //simulation time (in nanoseconds) (default: 30 seconds)
   private long simulationTime = 30000000000L;

   //allows execution without contention
   private boolean noContentionEnabled = false;

   private StatSampler statSampler;
   private long statsSamplingInterval = 0;

   public PutGetStressor() {}

   public Map<String, String> stress(CacheWrapper wrapper) {
      this.cacheWrapper = wrapper;

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
         Stresser stresser = new Stresser(threadIndex);
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


      BucketsKeysTreeSet bucketsKeysTreeSet = new BucketsKeysTreeSet();
      for(Stresser s : stresserList) {
         bucketsKeysTreeSet.addKeySet(s.keyGenerator.getBucketPrefix(), s.keyGenerator.getAllKeys(numberOfKeys));
      }
      cacheWrapper.saveKeysStressed(bucketsKeysTreeSet);
      log.info("Keys stressed saved");

      saveSamples();

      return stresserList;
   }

   private class Stresser extends Thread {

      private int threadIndex;
      private KeyGenerator keyGenerator;
      private Random privateRandomGenerator;

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
         this.privateRandomGenerator = new Random(System.nanoTime());
         this.keyGenerator = new KeyGenerator(slaveIdx, threadIndex, noContentionEnabled, bucketPrefix);
      }

      @Override
      public void run() {
         int i = 0;
         int operationLeft;
         boolean executionSuccessful;
         boolean commitSuccessful;
         boolean readOnlyTransaction;
         long startTx;
         long startCommit = 0;
         Object lastReadValue;
         String bucketId = keyGenerator.getBucketPrefix();

         try {
            startPoint.await();
            log.info("Starting thread: " + getName());
         } catch (InterruptedException e) {
            log.warn(e);
         }

         startTime = System.nanoTime();
         if(coordinatorParticipation || !cacheWrapper.isCoordinator()) {

            while(delta < simulationTime){

               operationLeft = opPerTx(lowerBoundOp,upperBoundOp,privateRandomGenerator);
               readOnlyTransaction = isNextTransactionReadOnly();

               startTx = System.nanoTime();

               cacheWrapper.startTransaction();
               log.trace("*** [" + getName() + "] new transaction: " + i + "***");

               try{
                  if (readOnlyTransaction) {
                     lastReadValue = executeReadOnlyTransaction(operationLeft, bucketId);
                  } else {
                     lastReadValue = executeWriteTransaction(operationLeft, bucketId);
                  }
                  executionSuccessful = true;
               } catch (TransactionExecutionFailedException e) {
                  lastReadValue = e.getLastValueRead();
                  if (log.isDebugEnabled()) {
                     log.debug("[" + getName() + "] Failed to execute transaction: " + e.getLocalizedMessage(), e);
                  } else {
                     log.warn("[" + getName() + "] Failed to execute transaction: " + e.getLocalizedMessage());
                  }
                  executionSuccessful = false;
               }

               try{
                  startCommit = System.nanoTime();
                  cacheWrapper.endTransaction(executionSuccessful);
                  commitSuccessful = true;
               }
               catch(Throwable e){
                  log.warn("[" + getName() + "] error committing transaction", e);
                  commitSuccessful = false;
               }

               long endCommit = System.nanoTime();

               log.trace("*** [" + getName() + "] end transaction: " + i++ + "***");

               long commitDuration = endCommit - startCommit;
               long execDuration = endCommit - startTx;

               //update stats
               if(executionSuccessful) {
                  if (commitSuccessful) {
                     if (readOnlyTransaction) {
                        readOnlyTxCommitSuccessDuration += commitDuration;
                        readOnlyTxDuration += execDuration;
                        numberOfReadOnlyTx++;
                     } else {
                        writeTxCommitSuccessDuration += commitDuration;
                        writeTxDuration += execDuration;
                        numberOfWriteTx++;
                     }
                  } else {
                     if (readOnlyTransaction) {
                        readOnlyTxCommitFailDuration += commitDuration;
                        commitFailedReadOnlyTxDuration += execDuration;
                        numberOfCommitFailedReadOnlyTx++;
                     } else {
                        writeTxCommitFailDuration += commitDuration;
                        commitFailedWriteTxDuration += execDuration;
                        numberOfCommitFailedWriteTx++;
                     }
                  }
               } else {
                  //it is a rollback                  
                  if (readOnlyTransaction) {
                     readOnlyTxTollbackDuration += commitDuration;
                     execFailedReadOnlyTxDuration += execDuration;
                     numberOfExecFailedReadOnlyTx++;
                  } else {
                     writeTxTollbackDuration += commitDuration;
                     execFailedWriteTxDuration += execDuration;
                     numberOfExecFailedWriteTx++;
                  }
               }

               this.delta = System.nanoTime() - startTime;
               //logProgress(i, lastReadValue);
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

      /**
       *
       * @return true if the transaction is a read-only transaction
       */
      private boolean isNextTransactionReadOnly() {
         return cacheWrapper.canExecuteReadOnlyTransactions() &&
               (!cacheWrapper.canExecuteWriteTransactions() || privateRandomGenerator.nextInt(100) >= writeTransactionPercentage);
      }

      private Object executeWriteTransaction(int operationLeft, String bucketId)
            throws TransactionExecutionFailedException {
         Object lastReadValue = null;
         int randomKeyInt;
         boolean alreadyWritten = false;

         while(operationLeft > 0){
            randomKeyInt = privateRandomGenerator.nextInt(numberOfKeys);
            String key = keyGenerator.getKey(randomKeyInt);

            if (isReadOperation(operationLeft, alreadyWritten)) {
               try {
                  lastReadValue = cacheWrapper.get(bucketId, key);
               } catch (Throwable e) {
                  TransactionExecutionFailedException tefe = new TransactionExecutionFailedException(
                        "Error while reading " + bucketId + ":" + key, e);
                  tefe.setLastValueRead(lastReadValue);
                  throw tefe;
               }
            } else {
               String payload = generateRandomString(sizeOfValue);

               try {
                  alreadyWritten = true;
                  cacheWrapper.put(bucketId, key, payload);
               } catch (Throwable e) {
                  TransactionExecutionFailedException tefe = new TransactionExecutionFailedException(
                        "Error while writing " + bucketId + ":" + key, e);
                  tefe.setLastValueRead(lastReadValue);
                  throw tefe;
               }

            }
            operationLeft--;
         }
         return lastReadValue;
      }

      private Object executeReadOnlyTransaction(int operationLeft, String bucketId)
            throws TransactionExecutionFailedException {
         Object lastReadValue = null;
         int randomKeyInt;

         while(operationLeft > 0){
            randomKeyInt = privateRandomGenerator.nextInt(numberOfKeys);
            String key = keyGenerator.getKey(randomKeyInt);

            try {
               lastReadValue = cacheWrapper.get(bucketId, key);
            } catch (Throwable e) {
               TransactionExecutionFailedException tefe = new TransactionExecutionFailedException(
                     "Error while reading " + bucketId + ":" + key, e);
               tefe.setLastValueRead(lastReadValue);
               throw tefe;
            }

            operationLeft--;
         }
         return lastReadValue;
      }

      private boolean isReadOperation(int operationLeft, boolean alreadyWritten) {
         //is a read operation when:
         // -- the random generate number is higher thant the write percentage... and...         
         // -- it is the last operation and at least one write operation was done.
         return privateRandomGenerator.nextInt(100) >= writeOperationPercentage &&
               !(operationLeft == 1 && !alreadyWritten);
      }

      private void logProgress(int i, Object result) {
         if ((i + 1) % opsCountStatusLog == 0) {
            long elapsedTime = System.nanoTime() - startTime;
            //this is printed here just to make sure JIT doesn't
            // skip the call to cacheWrapper.get
            log.info("Thread index '" + threadIndex + "' executed " + (i + 1) + " transactions. Elapsed time: " +
                           Utils.getDurationString((long) convertNanosToMillis(elapsedTime)) +
                           ". Last value read is " + result);
         }
      }

      private String generateRandomString(int size) {
         // each char is 2 bytes
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < size / 2; i++) sb.append((char) (64 + privateRandomGenerator.nextInt(26)));
         return sb.toString();
      }
   }

   private String str(Object o) {
      return String.valueOf(o);
   }

   private int opPerTx(int lower_bound, int upper_bound,Random ran){
      if(lower_bound == upper_bound)
         return lower_bound;
      return(ran.nextInt(upper_bound-lower_bound)+lower_bound);
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
      return "PutGetStressor{" +
            "opsCountStatusLog=" + opsCountStatusLog +
            ", numberOfKeys=" + numberOfKeys +
            ", sizeOfValue=" + sizeOfValue +
            ", writeOperationPercentage=" + writeOperationPercentage +
            ", writeTransactionPercentage=" + writeTransactionPercentage +
            ", numOfThreads=" + numOfThreads +
            ", bucketPrefix=" + bucketPrefix +
            ", coordinatorParticipation=" + coordinatorParticipation +
            ", lowerBoundOp=" + lowerBoundOp +
            ", upperBoundOp=" + upperBoundOp +
            ", simulationTime=" + simulationTime +
            ", noContentionEnabled=" + noContentionEnabled +
            ", cacheWrapper=" + cacheWrapper.getInfo() +
            "}";
   }

   /*
   * -----------------------------------------------------------------------------------
   * SETTERS
   * -----------------------------------------------------------------------------------
   */

   public void setLowerBoundOp(int lower_bound_op) {
      this.lowerBoundOp = lower_bound_op;
   }

   public void setUpperBoundOp(int upper_bound_op) {
      this.upperBoundOp = upper_bound_op;
   }

   public void setSlaveIdx(int slaveIdx) {
      this.slaveIdx = slaveIdx;
   }

   public void setSimulationTime(long simulationTime) {
      this.simulationTime = simulationTime*;
   }

   public void setNoContentionEnabled(boolean noContentionEnabled) {
      this.noContentionEnabled = noContentionEnabled;
   }

   public void setBucketPrefix(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix;
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

   public void setWriteOperationPercentage(int writeOperationPercentage) {
      this.writeOperationPercentage = writeOperationPercentage;
   }

   public void setOpsCountStatusLog(int opsCountStatusLog) {
      this.opsCountStatusLog = opsCountStatusLog;
   }

   public void setCoordinatorParticipation(boolean coordinatorParticipation) {
      this.coordinatorParticipation = coordinatorParticipation;
   }

   public void setWriteTransactionPercentage(int writeTransactionPercentage) {
      this.writeTransactionPercentage = writeTransactionPercentage;
   }

   public void setStatsSamplingInterval(long l){
      this.statsSamplingInterval = l;
   }
}

