package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.keygenerator.KeyGenerator.KeyGeneratorFactory;
import org.radargun.utils.BucketsKeysTreeSet;
import org.radargun.utils.StatSampler;
import org.radargun.utils.TransactionSelector;
import org.radargun.utils.Utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
   private int slaveIdx = 0;
   private int numberOfNodes = 1;
   private KeyGeneratorFactory factory;

   //for each there will be created fixed number of keys. All the GETs and PUTs are performed on these keys only.
   private int numberOfKeys = 1000;

   //Each key will be a byte[] of this size.
   private int sizeOfValue = 1000;   

   //The percentage of write transactions generated
   private volatile int writeTransactionPercentage = 100;

   //the number of threads that will work on this cache wrapper.
   private int numOfThreads = 1;

   //indicates that the coordinator execute or not txs -- PEDRO
   private boolean coordinatorParticipation = true;

   private String wrtOpsPerWriteTx = "100";

   private String rdOpsPerWriteTx = "100";

   private String rdOpsPerReadTx = "100";

   //simulation time (default: 30 seconds)
   private long simulationTime = 30L;

   //allows execution without contention
   private boolean noContentionEnabled = false;

   private StatSampler statSampler;
   private long statsSamplingInterval = 0;

   private final Timer stopBenchmarkTimer = new Timer("stop-benchmark-timer");

   private final AtomicBoolean running = new AtomicBoolean(true);

   private final List<Stresser> stresserList = new LinkedList<Stresser>();

   public PutGetStressor() {}

   public Map<String, String> stress(CacheWrapper wrapper) {
      this.cacheWrapper = wrapper;

      startTime = System.currentTimeMillis();
      log.info("Executing: " + this.toString());

      try {
         log.warn("Resetting statistics before the PutGetStressors start executing");
         wrapper.resetAdditionalStats();
         if(this.statsSamplingInterval > 0) {
            this.statSampler = new StatSampler(this.statsSamplingInterval);
         }
         executeOperations();
      } catch (Exception e) {
         log.warn("exception when stressing the cache wrapper", e);
         throw new RuntimeException(e);
      }
      return processResults();
   }

   public void destroy() throws Exception {
      cacheWrapper.empty();
      cacheWrapper = null;
   }

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

   private void executeOperations() throws Exception {
      startPoint = new CountDownLatch(1);

      if (factory != null) {
         factory.checkAndUpdate(numberOfNodes, numOfThreads, numberOfKeys, sizeOfValue, !noContentionEnabled,
                                bucketPrefix);
      } else {
         factory = new KeyGeneratorFactory(numberOfNodes, numOfThreads, numberOfKeys, sizeOfValue, !noContentionEnabled,
                                           bucketPrefix);
      }

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
      if (statSampler != null) {
         statSampler.cancel();
      }


      BucketsKeysTreeSet bucketsKeysTreeSet = new BucketsKeysTreeSet();
      for(Stresser s : stresserList) {
         bucketsKeysTreeSet.addKeySet(s.keyGenerator.getBucketPrefix(), s.keyGenerator.getAllKeys());
      }
      cacheWrapper.saveKeysStressed(bucketsKeysTreeSet);
      log.info("Keys stressed saved");

      saveSamples();
   }

   private class Stresser extends Thread {

      private int threadIndex;
      private org.radargun.keygenerator.KeyGenerator keyGenerator;
      private final Random privateRandomGenerator;
      private final TransactionSelector transactionSelector;

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
         this.transactionSelector = new TransactionSelector(privateRandomGenerator);
         this.transactionSelector.setReadOperationsForReadTx(rdOpsPerReadTx);
         this.transactionSelector.setReadOperationsForWriteTx(rdOpsPerWriteTx);
         this.transactionSelector.setWriteOperationsForWriteTx(wrtOpsPerWriteTx);         
         this.transactionSelector.setWriteTxPercentage(writeTransactionPercentage);
         this.keyGenerator = factory.constructForBenchmark(slaveIdx, threadIndex);
      }

      @Override
      public void run() {
         int i = 0;
         boolean executionSuccessful;
         boolean commitSuccessful;
         long startTx;
         long startCommit = 0;
         Object lastReadValue;         

         try {
            startPoint.await();
            log.info("Starting thread: " + getName());
         } catch (InterruptedException e) {
            log.warn(e);
         }

         startTime = System.nanoTime();
         if(coordinatorParticipation || !cacheWrapper.isCoordinator()) {

            while(running.get()){

               TransactionSelector.OperationIterator operationIterator = transactionSelector.chooseTransaction(cacheWrapper);

               startTx = System.nanoTime();

               cacheWrapper.startTransaction();
               log.trace("*** [" + getName() + "] new transaction: " + i + "***");

               try{
                  lastReadValue = executeTransaction(operationIterator);
                  executionSuccessful = true;
               } catch (TransactionExecutionFailedException e) {
                  lastReadValue = e.getLastValueRead();
                  logException(e, "Execution");
                  executionSuccessful = false;
               }

               try{
                  startCommit = System.nanoTime();
                  cacheWrapper.endTransaction(executionSuccessful);
                  commitSuccessful = true;
               }
               catch(Throwable e){
                  logException(e, "Commit");
                  commitSuccessful = false;
               }

               long endCommit = System.nanoTime();

               log.trace("*** [" + getName() + "] end transaction: " + i++ + "***");

               boolean readOnlyTransaction = operationIterator.isReadOnly();
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
               logProgress(i, lastReadValue);
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

      private void logException(Throwable e, String where) {
         String msg = "[" + getName() + "] exception caught in " + where + ": " + e.getLocalizedMessage();
         if (log.isDebugEnabled()) {
            log.debug(msg, e);
         } else {
            log.warn(msg);
         }
      }

      private Object executeTransaction(TransactionSelector.OperationIterator operationIterator)
            throws TransactionExecutionFailedException {
         Object lastReadValue = null;

         while(operationIterator.hasNext()){
            String key = keyGenerator.getRandomKey();

            if (operationIterator.isNextOperationARead()) {
               try {
                  lastReadValue = cacheWrapper.get(null, key);
               } catch (Throwable e) {
                  TransactionExecutionFailedException tefe = new TransactionExecutionFailedException(
                        "Error while reading " + key, e);
                  tefe.setLastValueRead(lastReadValue);
                  throw tefe;
               }
            } else {
               String payload = keyGenerator.getRandomValue();

               try {
                  cacheWrapper.put(null, key, payload);
               } catch (Throwable e) {
                  TransactionExecutionFailedException tefe = new TransactionExecutionFailedException(
                        "Error while writing " + key, e);
                  tefe.setLastValueRead(lastReadValue);
                  throw tefe;
               }

            }
         }
         return lastReadValue;
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

   private void finishBenchmark() {
      running.set(false);
   }

   public void stopBenchmark() {
      stopBenchmarkTimer.cancel();
      running.set(false);
   }

   @Override
   public String toString() {
      return "PutGetStressor{" +
            "opsCountStatusLog=" + opsCountStatusLog +
            ", numberOfKeys=" + numberOfKeys +
            ", sizeOfValue=" + sizeOfValue +            
            ", writeTransactionPercentage=" + writeTransactionPercentage +
            ", numOfThreads=" + numOfThreads +
            ", bucketPrefix=" + bucketPrefix +
            ", coordinatorParticipation=" + coordinatorParticipation +
            ", wrtOpsPerWriteTx=" + wrtOpsPerWriteTx + 
            ", rdOpsPerWriteTx=" + rdOpsPerWriteTx +
            ", rdOpsPerReadTx=" + rdOpsPerReadTx +            
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

   public void setWrtOpsPerWriteTx(String wrtOpsPerWriteTx) {
      this.wrtOpsPerWriteTx = wrtOpsPerWriteTx;
      for (Stresser stresser : stresserList) {
         stresser.transactionSelector.setWriteOperationsForWriteTx(wrtOpsPerWriteTx);
      }
   }

   public void setRdOpsPerWriteTx(String rdOpsPerWriteTx) {
      this.rdOpsPerWriteTx = rdOpsPerWriteTx;
      for (Stresser stresser : stresserList) {
         stresser.transactionSelector.setReadOperationsForWriteTx(rdOpsPerWriteTx);
      }
   }

   public void setRdOpsPerReadTx(String rdOpsPerReadTx) {
      this.rdOpsPerReadTx = rdOpsPerReadTx;
      for (Stresser stresser : stresserList) {
         stresser.transactionSelector.setReadOperationsForReadTx(rdOpsPerReadTx);
      }
   }   

   public void setSlaveIdx(int slaveIdx) {
      this.slaveIdx = slaveIdx;
   }

   public void setNumberOfNodes(int numberOfNodes) {
      this.numberOfNodes = numberOfNodes;
   }

   //NOTE this time is in seconds!
   public void setSimulationTime(long simulationTime) {
      this.simulationTime = simulationTime;
   }

   public void setNoContentionEnabled(boolean noContentionEnabled) {
      this.noContentionEnabled = noContentionEnabled;
   }

   public void setBucketPrefix(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix;
   }

   public void setNumberOfKeys(int numberOfKeys) {
      this.numberOfKeys = numberOfKeys;
      for (Stresser stresser : stresserList) {
         stresser.keyGenerator.setNumberOfKeys(numberOfKeys);
      }
   }

   public void setSizeOfValue(int sizeOfValue) {
      this.sizeOfValue = sizeOfValue;
   }

   public void setNumOfThreads(int numOfThreads) {
      this.numOfThreads = numOfThreads;
   }   

   public void setOpsCountStatusLog(int opsCountStatusLog) {
      this.opsCountStatusLog = opsCountStatusLog;
   }

   public void setCoordinatorParticipation(boolean coordinatorParticipation) {
      this.coordinatorParticipation = coordinatorParticipation;
   }

   public void setWriteTransactionPercentage(int writeTransactionPercentage) {
      this.writeTransactionPercentage = writeTransactionPercentage;
      for (Stresser stresser : stresserList) {
         stresser.transactionSelector.setWriteTxPercentage(writeTransactionPercentage);
      }
   }

   public void setStatsSamplingInterval(long l){
      this.statsSamplingInterval = l;
   }

   public void setFactory(org.radargun.keygenerator.KeyGenerator.KeyGeneratorFactory factory) {
      this.factory = factory;
   }
}

