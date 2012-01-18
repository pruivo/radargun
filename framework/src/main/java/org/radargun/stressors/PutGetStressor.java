package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.utils.Utils;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * On multiple threads executes put and get operations against the CacheWrapper, and returns the result as an Map.
 *
 * @author Mircea.Markus@jboss.com
 * changed by:
 * @author Pedro Ruivo
 */
public class PutGetStressor implements CacheWrapperStressor {

    public static final String DEFAULT_BUCKET_PREFIX = "BUCKET_";
    public static final String DEFAULT_KEY_PREFIX = "KEY_";
    private static Log log = LogFactory.getLog(PutGetStressor.class);

    private CacheWrapper cacheWrapper;
    private static Random r = new Random();
    private long startTime;
    private volatile CountDownLatch startPoint;
    private String bucketPrefix = DEFAULT_BUCKET_PREFIX;
    private int opsCountStatusLog = 5000;
    private int slaveIdx = 1;

    //for each there will be created fixed number of keys. All the GETs and PUTs are performed on these keys only.
    private int numberOfKeys = 1000;

    //Each key will be a byte[] of this size.
    private int sizeOfValue = 1000;

    //Out of the total number of operations, this defines the frequency of writes (percentage).
    private int writePercentage = 20;

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

    //allows read only transaction
    private boolean readOnlyTransactionsEnabled = false;

    //allows execution without contention
    private boolean noContentionEnabled = false;

    public PutGetStressor() {}

    public Map<String, String> stress(CacheWrapper wrapper) {
        this.cacheWrapper = wrapper;

        startTime = System.currentTimeMillis();
        log.info("Executing: " + this.toString());

        List<Stresser> stressers;
        try {
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

        //number of successful tx
        int numberOfReadOnlyTx = 0;
        int numberOfWriteTx = 0;

        //number of rollback exception
        int numberOfFailedTx = 0;

        //duration of read, write txs and commit
        long allReadOnlyTxDuration = 0;
        long allWriteTxDuration = 0;
        long allSuccessCommitDuration = 0;
        long allFailedCommitDuration = 0;
        long allCommitDuration = 0;

        int numberOfCommitFailed = 0;

        for (Stresser stresser : stressers) {
            totalDuration += stresser.delta;
            allReadOnlyTxDuration += stresser.allReadOnlyTxDuration;
            allWriteTxDuration += stresser.allWriteTxDuration;
            allSuccessCommitDuration+=stresser.commitSuccessDuration;
            allFailedCommitDuration += stresser.commitFailDuration;
            allCommitDuration += stresser.commitTotalDuration;

            numberOfReadOnlyTx += stresser.numberOfReadOnlyTx;
            numberOfWriteTx += stresser.numberOfWriteTx;

            numberOfCommitFailed += stresser.nrFailsAtCommit;

            numberOfFailedTx += stresser.nrFailures;
        }

        totalDuration /= 1000000; //nanos to mili
        allSuccessCommitDuration /= 1000; //nanos to micros
        allFailedCommitDuration /= 1000; //nanos to micros
        allCommitDuration /= 1000; //nanos to micros

        Map<String, String> results = new LinkedHashMap<String, String>();

        double requestPerSec = (numberOfReadOnlyTx + numberOfWriteTx)  /((totalDuration /numOfThreads) / 1000.0);
        double writeTxPerSec=0;
        double readTxPerSec=0;

        results.put("DURATION (msec)", str(totalDuration /numOfThreads));
        results.put("REQ_PER_SEC", str(requestPerSec));

        if(allReadOnlyTxDuration == 0) {
            results.put("READS_PER_SEC",str(0));
        } else{
            readTxPerSec=numberOfReadOnlyTx / ((allReadOnlyTxDuration/numOfThreads) / 1000.0);
            results.put("READS_PER_SEC", str(readTxPerSec));
        }

        if (allWriteTxDuration == 0) {
            results.put("WRITES_PER_SEC", str(0));
        } else{
            writeTxPerSec=numberOfWriteTx / ((allWriteTxDuration/numOfThreads) / 1000.0);
            results.put("WRITES_PER_SEC", str(writeTxPerSec));
        }

        results.put("READ_COUNT", str(numberOfReadOnlyTx));
        results.put("WRITE_COUNT", str(numberOfWriteTx));
        results.put("FAILURES", str(numberOfFailedTx));
        results.put("FAILURES_COMMIT", str(numberOfCommitFailed));

        if(requestPerSec!=0) {
            results.put("AVG_TX_DURATION (sec)",str((1/requestPerSec)*numOfThreads));
        } else {
            results.put("AVG_TX_DURATION (sec)",str(0));
        }

        if(readTxPerSec!=0) {
            results.put("AVG_RD_TX_DURATION (sec)",str((1/readTxPerSec)*numOfThreads));
        } else {
            results.put("AVG_RD_TX_DURATION (sec)",str(0));
        }

        if(writeTxPerSec!=0) {
            results.put("AVG_WR_TX_DURATION (sec)",str((1/writeTxPerSec)*numOfThreads));
        } else {
            results.put("AVG_WR_TX_DURATION (sec)",str(0));
        }

        if(numberOfWriteTx != 0) {
            results.put("AVG_SUCCESS_COMMIT_DURATION (usec)",str((allSuccessCommitDuration/numberOfWriteTx)));
        } else {
            results.put("AVG_SUCCESS_COMMIT_DURATION (usec)",str(0));
        }

        if(numberOfCommitFailed != 0) {
            results.put("AVG_FAILED_COMMIT_DURATION (usec)",str((allFailedCommitDuration/numberOfCommitFailed)));
        } else {
            results.put("AVG_FAILED_COMMIT_DURATION (usec)",str(0));
        }

        int totalCommits = numberOfWriteTx + numberOfReadOnlyTx + numberOfCommitFailed;
        if(totalCommits != 0) {
            results.put("AVG_ALL_COMMIT_DURATION (usec)",str((allCommitDuration/totalCommits)));
        } else {
            results.put("AVG_ALL_COMMIT_DURATION (usec)",str(0));
        }

        results.putAll(cacheWrapper.getAdditionalStats());

        log.info("Finished generating report. Nr of failed operations on this node is: " + numberOfFailedTx +
                ". Test duration is: " + Utils.getDurationString(System.currentTimeMillis() - startTime));
        return results;
    }

    private List<Stresser> executeOperations() throws Exception {
        List<Stresser> stressers = new ArrayList<Stresser>();
        startPoint = new CountDownLatch(1);

        for (int threadIndex = 0; threadIndex < numOfThreads; threadIndex++) {
            Stresser stresser = new Stresser(threadIndex);
            stresser.initializeKeys();
            stressers.add(stresser);

            try{
                stresser.start();
            }
            catch (Throwable t){
                log.warn("Error starting all the stressers", t);
            }
        }

        log.info("Cache private class Stresser extends Thread { wrapper info is: " + cacheWrapper.getInfo());
        startPoint.countDown();
        for (Stresser stresser : stressers) {
            stresser.join();
            log.info("stresser[" + stresser.getName() + "] finsihed");
        }
        log.info("****BARRIER JOIN PASSED****");



        Set<String> pooledKeys = new HashSet<String>(numberOfKeys);
        for(Stresser s : stressers) {
            pooledKeys.addAll(s.pooledKeys);
        }
        cacheWrapper.setStressedKeys(pooledKeys);

        return stressers;
    }

    private class Stresser extends Thread {

        private ArrayList<String> pooledKeys = new ArrayList<String>(numberOfKeys);

        private int threadIndex;
        private String bucketId;
        private int nrFailures = 0;
        private int nrFailsAtCommit = 0;
        private long allReadOnlyTxDuration = 0;
        private long allWriteTxDuration = 0;
        private long numberOfReadOnlyTx = 0;
        private long numberOfWriteTx = 0;
        private long startTime = 0;


        private long delta=0;

        private long commitSuccessDuration = 0;
        private long commitFailDuration = 0;
        private long commitTotalDuration = 0;

        private Random operationTypeRandomGenerator;

        public Stresser(int threadIndex) {
            super("Stresser-" + threadIndex);
            this.threadIndex = threadIndex;
            this.bucketId = getBucketId(threadIndex);
            this.operationTypeRandomGenerator = new Random(System.nanoTime());
        }

        @Override
        public void run() {
            int randomKeyInt;
            int i = 0;
            int operationLeft;
            boolean successful;
            long start;
            long init_time = System.nanoTime();
            long commit_start = 0;
            boolean alreadyWritten;
            Object lastReadValue = null;

            try {
                startPoint.await();
                log.info("Starting thread: " + getName());
            } catch (InterruptedException e) {
                log.warn(e);
            }

            if(coordinatorParticipation || !cacheWrapper.isCoordinator()) {

                while(delta < simulationTime){

                    successful = true;
                    alreadyWritten = false;
                    operationLeft = opPerTx(lowerBoundOp,upperBoundOp,r);

                    start = System.currentTimeMillis();

                    cacheWrapper.startTransaction();
                    log.info("*** [" + getName() + "] new transaction: " + i + "***");

                    while(operationLeft > 0 && successful){
                        randomKeyInt = r.nextInt(numberOfKeys - 1);
                        String key = getKey(randomKeyInt);

                        if (isReadOperation(operationLeft, alreadyWritten)) {
                            try {
                                lastReadValue = cacheWrapper.get(bucketId, key);
                            } catch (Throwable e) {
                                log.warn("[" + getName() + "] error in get operation", e);
                                successful = false;
                            }
                        } else {
                            String payload = generateRandomString(sizeOfValue);

                            try {
                                alreadyWritten = true;
                                cacheWrapper.put(bucketId, key, payload);
                            } catch (Throwable e) {
                                successful = false;
                                log.warn("[" + getName() + "] error in put operation", e);
                            }

                        }
                        operationLeft--;
                    }

                    //here we try to finalize the transaction
                    //if any read/write has failed we abort
                    boolean measureTime = false;
                    try{
                        if(successful){
                            commit_start=System.nanoTime();
                            measureTime = true;
                        }
                        cacheWrapper.endTransaction(successful);
                        if(!successful){
                            nrFailures++;
                        }
                    }
                    catch(Throwable e){
                        log.warn("[" + getName() + "] error committing transaction", e);
                        nrFailures++;
                        nrFailsAtCommit++;
                        successful = false;
                    }

                    long commit_end = System.nanoTime();

                    if(measureTime) {
                        if(successful) {
                            this.commitSuccessDuration += commit_end - commit_start;
                        } else {
                            this.commitFailDuration += commit_end - commit_start;
                        }
                        this.commitTotalDuration += commit_end - commit_start;
                    }

                    log.info("*** [" + getName() + "] end transaction: " + i++ + "***");

                    if(alreadyWritten){
                        allWriteTxDuration += System.currentTimeMillis() - start;
                        if(successful) {
                            numberOfWriteTx++;
                        }
                    }
                    else{    //ro transaction
                        allReadOnlyTxDuration += System.currentTimeMillis() - start;
                        if(successful) {
                            numberOfReadOnlyTx++;
                        }
                    }
                    this.delta = System.nanoTime() - init_time;
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

            try {
                Thread.sleep(30000); //30 seconds
            } catch (InterruptedException e) {
                log.info("interrupted exception catched in the shutdown interval");
            }
        }

        private boolean isReadOperation(int operationLeft, boolean alreadyWritten) {
            //is a read operation when:
            // -- the random generate number is higher thant the write percentage... and...
            // -- read only transaction are allowed... or...
            // -- it is the last operation and at least one write operation was done.
            return operationTypeRandomGenerator.nextInt(100) >= writePercentage &&
                    (readOnlyTransactionsEnabled || !(operationLeft == 1 && !alreadyWritten));
        }

        private void logProgress(int i, Object result) {
            if ((i + 1) % opsCountStatusLog == 0) {
                double elapsedTime = System.currentTimeMillis() - startTime;
                //this is printed here just to make sure JIT doesn't
                // skip the call to cacheWrapper.get
                log.info("Thread index '" + threadIndex + "' executed " + (i + 1) + " transactions. Elapsed time: " +
                        Utils.getDurationString((long) elapsedTime) + ". Last vlue read is " + result);
            }
        }

        public void initializeKeys() {
            for (int keyIndex = 0; keyIndex < numberOfKeys; keyIndex++) {
                String key;
                if(noContentionEnabled) {
                    key = DEFAULT_KEY_PREFIX + slaveIdx + "_" + threadIndex + "_" + keyIndex;
                } else {
                    key = DEFAULT_KEY_PREFIX + keyIndex;
                }
                pooledKeys.add(key);
            }
        }

        private String getKey(int keyIndex) {
            return pooledKeys.get(keyIndex);
        }
    }

    private String str(Object o) {
        return String.valueOf(o);
    }

    /*
    * This will make sure that each session runs in its own thread and no collisition will take place. See
    * https://sourceforge.net/apps/trac/cachebenchfwk/ticket/14
    */
    private String getBucketId(int threadIndex) {
        return bucketPrefix + "_" + threadIndex;
    }

    private static String generateRandomString(int size) {
        // each char is 2 bytes
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size / 2; i++) sb.append((char) (64 + r.nextInt(26)));
        return sb.toString();
    }


    private int opPerTx(int lower_bound, int upper_bound,Random ran){
        if(lower_bound==upper_bound)
            return lower_bound;
        return(ran.nextInt(upper_bound-lower_bound)+lower_bound);
    }

    @Override
    public String toString() {
        return "PutGetStressor{" +
                "opsCountStatusLog=" + opsCountStatusLog +
                ", numberOfKeys=" + numberOfKeys +
                ", sizeOfValue=" + sizeOfValue +
                ", writePercentage=" + writePercentage +
                ", numOfThreads=" + numOfThreads +
                ", bucketPrefix=" + bucketPrefix +
                ", coordinatorParticipation=" + coordinatorParticipation +
                ", lowerBoundOp=" + lowerBoundOp +
                ", upperBoundOp=" + upperBoundOp +
                ", simulationTime=" + simulationTime +
                ", readOnlyTransactionsEnabled=" + readOnlyTransactionsEnabled +
                ", noContentionEnabled=" + noContentionEnabled +
                ", cacheWrapper=" + cacheWrapper +

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
        this.simulationTime = simulationTime;
    }

    public void setReadOnlyTransactionsEnabled(boolean readOnlyTransactionsEnabled) {
        this.readOnlyTransactionsEnabled = readOnlyTransactionsEnabled;
    }

    public void setNoContentionEnabled(boolean noContentionEnabled) {
        this.noContentionEnabled = noContentionEnabled;
    }

    public void setBucketPrefix(String bucketPrefix) {
        this.bucketPrefix = bucketPrefix;
    }

    public void setNumberOfAttributes(int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
    }

    public void setSizeOfAnAttribute(int sizeOfValue) {
        this.sizeOfValue = sizeOfValue;
    }

    public void setNumOfThreads(int numOfThreads) {
        this.numOfThreads = numOfThreads;
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
}

