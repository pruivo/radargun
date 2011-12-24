package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.utils.Utils;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * On multiple threads executes put and get operations against the CacheWrapper, and returns the result as an Map.
 *
 * @author Mircea.Markus@jboss.com
 */
public class PutGetStressor implements CacheWrapperStressor {

    private static Log log = LogFactory.getLog(PutGetStressor.class);

    private int opsCountStatusLog = 5000;

    public static final String DEFAULT_BUCKET_PREFIX = "BUCKET_";
    public static final String DEFAULT_KEY_PREFIX = "KEY_";


    /**
     * total number of operation to be made against cache wrapper: reads + writes
     */
    private int numberOfRequests = 5000;

    /**
     * for each there will be created fixed number of keys. All the GETs and PUTs are performed on these keys only.
     */
    private int numberOfKeys = 1000;

    /**
     * Each key will be a byte[] of this size.
     */
    private int sizeOfValue = 1000;

    /**
     * Out of the total number of operations, this defines the frequency of writes (percentage).
     */
    private int writePercentage = 20;


    /**
     * the number of threads that will work on this cache wrapper.
     */
    private int numOfThreads = 1;

    //indicates that the coordinator execute or not txs -- PEDRO
    private boolean coordinatorParticipation = true;

    private String bucketPrefix = DEFAULT_BUCKET_PREFIX;


    private CacheWrapper cacheWrapper;
    private static Random r = new Random();
    private long startTime;
    private volatile CountDownLatch startPoint;

    //Diego
    private boolean is_master=false;
    private int total_num_of_slaves;
    private int lower_bound_op;
    private int upper_bound_op;
    private long simulTime;

    //Pedro
    private Set<String> pooledKeys = new HashSet<String>(numberOfKeys);
    private Map<String, Object> finalValues = new HashMap<String, Object>();


    //Diego
    public PutGetStressor(boolean b, int slaves, int lb, int ub, long simulTime){
        this.is_master=b;
        this.total_num_of_slaves=slaves;
        this.lower_bound_op=lb;
        this.upper_bound_op=ub;
        this.simulTime=simulTime;
    }

    public PutGetStressor() {
        this.is_master = true;
        this.total_num_of_slaves = 1;
        this.simulTime = 30000000;
    }

    public boolean isMaster(){
        return this.is_master;
    }

    public Map<String, String> stress(CacheWrapper wrapper) {
        this.cacheWrapper = wrapper;

        startTime = System.currentTimeMillis();
        log.info("Executing: " + this.toString());

        List<Stresser> stressers;
        try {
            stressers = executeOperations();
        } catch (Exception e) {
            log.info("***** Eccezione nel metodo PutGetStressor.stress durante la chiamata a  executeOperations()*****");
            throw new RuntimeException(e);
        }
        return processResults(stressers);
    }

    public void destroy() throws Exception {
        cacheWrapper.empty();
        cacheWrapper = null;
    }

    public Map<String, Object> getAllKeys() {
        return finalValues;
    }

    private Map<String, String> processResults(List<Stresser> stressers) {
        //total duration
        long duration = 0;

        //number of sucessful tx
        int reads = 0;
        int writes = 0;

        //number of rollback exception
        int failures = 0;
        int writeSkew = 0;
        int localTimeout=0;
        int dldExec = 0;

        //duration of read, write txs and commit
        long readsDurations = 0;
        long writesDurations = 0;
        long successCommitDurations = 0;
        long failCommitDurations = 0;
        long allCommitDurations = 0;

        long remote_tout=0;

        //contentation: local_vs_local, local_vs_remote, remote_vs_local, remote_vs_remote
        int LL = -1;
        int LR = -1;
        int RL = -1;
        int RR = -1;

        int abortValidationAbcast = -1;
        int abortRemoteConflit2PC = -1;
        int abortIncomingRemoteAbcast = -1;

        long abcastSelfDeliverDur = -1;
        long abcastValidationDur = -1;
        int numOfAbcastMsgs = -1;

        long stealLockTime = -1;
        int stealLockNumberOfTries = -1;
        int numberOfTxThiefs = -1;

        long flushDuration = -1;
        long rollDuration = -1;

        int allCommittedTx = -1;
        int allRollbackedTx = -1;


        //jgroup's sequencer stats
        long abcastSelfDelJG = -1;
        long numMsgsJG = -1;

        String cacheMode = cacheWrapper.getCacheMode().toLowerCase();

        try {
            MBeanServer threadMBeanServer = ManagementFactory.getPlatformMBeanServer();

            ObjectName lockMan = new ObjectName("org.infinispan"+":type=Cache"+",name="+ObjectName.quote("x(" + cacheMode + ")")+",manager="+ObjectName.quote("DefaultCacheManager")+",component=LockManager");
            if(!threadMBeanServer.isRegistered(lockMan)) {
                lockMan = new ObjectName("org.infinispan"+":type=Cache"+",name="+ObjectName.quote("x(" + cacheMode + ")")+",manager="+ObjectName.quote("DefaultCacheManager")+",component=DeadlockDetectingLockManager");
            }

            ObjectName txInter = new ObjectName("org.infinispan"+":type=Cache"+",name="+ObjectName.quote("x(" + cacheMode + ")")+",manager="+ObjectName.quote("DefaultCacheManager")+",component=Transactions");
            if(!threadMBeanServer.isRegistered(txInter)) {
                txInter = new ObjectName("org.infinispan"+":type=Cache"+",name="+ObjectName.quote("x(" + cacheMode + ")")+",manager="+ObjectName.quote("DefaultCacheManager")+",component=DistTxInterceptor");
            }

            ObjectName replInter= new ObjectName("org.infinispan"+":type=Cache"+",name="+ObjectName.quote("x(" + cacheMode + ")")+",manager="+ ObjectName.quote("DefaultCacheManager")+",component=ReplicationInterceptor");
            if(!threadMBeanServer.isRegistered(replInter)) {
                replInter = new ObjectName("org.infinispan"+":type=Cache"+",name="+ObjectName.quote("x(" + cacheMode + ")")+",manager="+ObjectName.quote("DefaultCacheManager")+",component=DistributionInterceptor");
            }

            ObjectName lockInter = new ObjectName("org.infinispan"+":type=Cache"+",name="+ObjectName.quote("x(" + cacheMode + ")")+",manager="+ObjectName.quote("DefaultCacheManager")+",component=LockingInterceptor");
            if(!threadMBeanServer.isRegistered(lockInter)) {
                lockInter = new ObjectName("org.infinispan"+":type=Cache"+",name="+ObjectName.quote("x(" + cacheMode + ")")+",manager="+ObjectName.quote("DefaultCacheManager")+",component=DistLockingInterceptor");
            }

            LL=((Long)threadMBeanServer.getAttribute(lockMan,"LocalLocalContentions")).intValue();
            LR=((Long)threadMBeanServer.getAttribute(lockMan,"LocalRemoteContentions")).intValue();
            RL=((Long)threadMBeanServer.getAttribute(lockMan,"RemoteLocalContentions")).intValue();
            RR=((Long)threadMBeanServer.getAttribute(lockMan,"RemoteRemoteContentions")).intValue();

            abortValidationAbcast = ((Long)threadMBeanServer.getAttribute(txInter, "AbortsValidation")).intValue();
            abortIncomingRemoteAbcast = ((Long)threadMBeanServer.getAttribute(replInter, "AbortsAbcast")).intValue();
            abortRemoteConflit2PC = ((Long)threadMBeanServer.getAttribute(replInter, "Aborts2PC")).intValue();

            abcastSelfDeliverDur = (Long)threadMBeanServer.getAttribute(txInter, "AbcastSelfDeliverDuration");
            abcastValidationDur = (Long)threadMBeanServer.getAttribute(txInter, "AbcastValidationDuration");
            numOfAbcastMsgs = ((Long)threadMBeanServer.getAttribute(txInter, "NumAbcastMsgs")).intValue();

            flushDuration = ((Long)threadMBeanServer.getAttribute(txInter, "FlushDuration"));
            rollDuration = ((Long)threadMBeanServer.getAttribute(txInter, "RollbackDuration"));

            allCommittedTx = ((Long)threadMBeanServer.getAttribute(txInter, "AllCommittedTx")).intValue();
            allRollbackedTx = ((Long)threadMBeanServer.getAttribute(txInter, "AllRollbackedTx")).intValue();

            stealLockTime = ((Long)threadMBeanServer.getAttribute(lockInter, "StealLockTime"));
            stealLockNumberOfTries = ((Long)threadMBeanServer.getAttribute(lockInter, "StealLockNumberOfTries")).intValue();
            numberOfTxThiefs = ((Long)threadMBeanServer.getAttribute(lockInter, "TxThief")).intValue();
        } catch (Exception e) {
            log.error(e);
        }

        cacheWrapper.printStatsFromStreamLib();

        int commitFails = 0;

        for (Stresser stresser : stressers) {
            duration+=stresser.delta();
            readsDurations += stresser.readDuration;
            writesDurations += stresser.writeDuration;
            successCommitDurations+=stresser.commitSuccessDuration;
            failCommitDurations += stresser.commitFailDuration;
            allCommitDurations += stresser.commitTotalDuration;

            reads += stresser.reads;
            writes += stresser.writes;

            commitFails += stresser.nrFailsAtCommit;

            failures += stresser.nrFailures;
            writeSkew += stresser.numWriteSkew;
            localTimeout += stresser.numLocalTimeOut;
            dldExec += stresser.numDlD;

            remote_tout += stresser.numRemoteTimeOut;
        }

        duration /= 1000000; //nanos to mili
        successCommitDurations /= 1000; //nanos to micros
        failCommitDurations /= 1000; //nanos to micros
        allCommitDurations /= 1000; //nanos to micros
        abcastSelfDeliverDur /= 1000; //nanos to micros
        abcastValidationDur /= 1000; //nanos to micros
        stealLockTime /= 1000; //nanos to micros
        flushDuration /= 1000; //nanos to micros
        rollDuration /= 1000; //nanos to micros

        Map<String, Object> jgroupsStats = cacheWrapper.dumpTransportStats();
        Map<String, Object> sequencerStats = (Map<String, Object>) jgroupsStats.get("SEQUENCER");
        //Map<String, Object> tpStats = (Map<String, Object>) jgroupsStats.get("UDP");

        if(sequencerStats != null) {
            Long temp = (Long) sequencerStats.get("self_deliver_time");
            if(temp != null) {
                abcastSelfDelJG = temp;
                abcastSelfDelJG /= 1000; //nanos to micros
            }

            temp = (Long) sequencerStats.get("number_of_messages");
            if(temp != null) {
                numMsgsJG = temp;
            }
        }

        /*if(tpStats != null) {
            System.out.println("----------TP--------------");
            System.out.println("num msg sent=" + tpStats.get("num_msgs_sent") +
                    "\nnum bytes sent=" + tpStats.get("num_bytes_sent") +
                    "\nnum msg recv=" + tpStats.get("num_msgs_received") +
                    "\nnum bytes recv=" + tpStats.get("num_bytes_received"));
            System.out.println("----------TP--------------");
        } else {
            System.out.println("----------TP IS NULL--------------");
            System.out.println(jgroupsStats);
        }*/

        Map<String, String> results = new LinkedHashMap<String, String>();

        double requestPerSec = (reads + writes)  /((duration/numOfThreads) / 1000.0);
        double wrtPerSec=0;
        double rdPerSec=0;

        results.put("DURATION (msec)", str(duration/numOfThreads));
        results.put("REQ_PER_SEC", str(requestPerSec));

        if(readsDurations==0) {
            results.put("READS_PER_SEC",str(0));
        } else{
            rdPerSec=reads   / ((readsDurations/numOfThreads) / 1000.0);
            results.put("READS_PER_SEC", str(rdPerSec));
        }

        if (writesDurations==0) {
            results.put("WRITES_PER_SEC", str(0));
        } else{
            wrtPerSec=writes / ((writesDurations/numOfThreads) / 1000.0);
            results.put("WRITES_PER_SEC", str(wrtPerSec));
        }

        results.put("READ_COUNT", str(reads));
        results.put("WRITE_COUNT", str(writes));
        results.put("FAILURES", str(failures));
        results.put("FAILURES_COMMIT", str(commitFails));

        if(requestPerSec!=0) {
            results.put("AVG_TX_DURATION (sec)",str((1/requestPerSec)*numOfThreads));
        } else {
            results.put("AVG_TX_DURATION (sec)",str(0));
        }

        if(rdPerSec!=0) {
            results.put("AVG_RD_TX_DURATION (sec)",str((1/rdPerSec)*numOfThreads));
        } else {
            results.put("AVG_RD_TX_DURATION (sec)",str(0));
        }

        if(wrtPerSec!=0) {
            results.put("AVG_WR_TX_DURATION (sec)",str((1/wrtPerSec)*numOfThreads));
        } else {
            results.put("AVG_WR_TX_DURATION (sec)",str(0));
        }

        if(writes != 0) {
            results.put("AVG_SUCCESS_COMMIT_DURATION (usec)",str((successCommitDurations/writes)));
        } else {
            results.put("AVG_SUCCESS_COMMIT_DURATION (usec)",str(0));
        }

        if(commitFails != 0) {
            results.put("AVG_FAILED_COMMIT_DURATION (usec)",str((failCommitDurations/commitFails)));
        } else {
            results.put("AVG_FAILED_COMMIT_DURATION (usec)",str(0));
        }

        int totalCommits = writes + reads + commitFails;
        if(totalCommits != 0) {
            results.put("AVG_ALL_COMMIT_DURATION (usec)",str((allCommitDurations/totalCommits)));
        } else {
            results.put("AVG_ALL_COMMIT_DURATION (usec)",str(0));
        }

        results.put("CONTENTION_LOCALvsLOCAL",str(LL));
        results.put("CONTENTION_LOCALvsREMOTE",str(LR));
        results.put("CONTENTION_REMOTEvsLOCAL",str(RL));
        results.put("CONTENTION_REMOTEvsREMOTE",str(RR));

        results.put("ABORT_DEAD_LOCK_EXEC", str(dldExec));
        results.put("ABORT_WRITE_SKEW_EXEC", str(writeSkew));
        results.put("ABORT_LOCAL_TIMEOUT_EXEC", str(localTimeout));
        results.put("ABORT_VALIDATION (ABCAST)", str(abortValidationAbcast));
        results.put("ABORT_INCOMING_REMOTE (ABCAST)", str(abortIncomingRemoteAbcast));
        results.put("ABORT_REMOTE_PREPARE_MSG", str(abortRemoteConflit2PC));

        if(numOfAbcastMsgs > 0) {
            results.put("ABCAST_SELF-DELIVER_TIME_PER_MSG (usec)", str(abcastSelfDeliverDur / numOfAbcastMsgs));
        } else {
            results.put("ABCAST_SELF-DELIVER_TIME_PER_MSG (usec)", str(0));
        }

        if(numMsgsJG > 0) {
            results.put("ABCAST_SELF-DELIVER_TIME_PER_MSG_JGROUPS (usec)", str(abcastSelfDelJG / numMsgsJG));
        } else {
            results.put("ABCAST_SELF-DELIVER_TIME_PER_MSG_JGROUPS (usec)", str(0));
        }

        int allTx = allCommittedTx + allRollbackedTx;
        if(allTx > 0) {
            results.put("VALIDATION_DURATION_ABCAST_PER_TX (usec)", str(abcastValidationDur / allTx));
        } else {
            results.put("VALIDATION_DURATION_ABCAST_PER_TX (usec)", str(0));
        }

        if(stealLockNumberOfTries > 0) {
            results.put("AVG_STEAL_LOCK_TIME (usec)",str(stealLockTime / stealLockNumberOfTries));
        } else {
            results.put("AVG_STEAL_LOCK_TIME (usec)",str(0));
        }

        results.put("NUMBER_OF_STEAL_LOCKS",str(stealLockNumberOfTries));
        results.put("TX_THAT_STOLEN_LOCK", str(numberOfTxThiefs));

        if(allCommittedTx > 0) {
            results.put("FLUSH_DURATION (usec)", str(flushDuration / allCommittedTx));
        } else {
            results.put("FLUSH_DURATION (usec)", str(0));
        }

        if(allRollbackedTx > 0) {
            results.put("ROLLBACK_DURATION (usec)", str(rollDuration / allRollbackedTx));
        } else {
            results.put("ROLLBACK_DURATION (usec)", str(0));
        }

        results.put("ALL_COMMITTED_TX", str(allCommittedTx));
        results.put("ALL_ROLLBACKED_TX", str(allRollbackedTx));

        log.info("Finished generating report. Nr of failed operations on this node is: " + failures +
                ". Test duration is: " + Utils.getDurationString(System.currentTimeMillis() - startTime));
        return results;
    }

    private List<Stresser> executeOperations() throws Exception {
        List<Stresser> stressers = new ArrayList<Stresser>();
        final AtomicInteger requestsLeft = new AtomicInteger(numberOfRequests);       //now it is number of tx
        startPoint = new CountDownLatch(1);



        for (int threadIndex = 0; threadIndex < numOfThreads; threadIndex++) {
            Stresser stresser = new Stresser(threadIndex, requestsLeft,this.is_master,this.lower_bound_op,this.upper_bound_op,this.simulTime);
            stresser.initializeKeys();
            stressers.add(stresser);

            try{
                stresser.start();
            }
            catch (Throwable t){

                log.info("***Eccezione nella START "+t);
            }
        }

        log.info("Cache private class Stresser extends Thread { wrapper info is: " + cacheWrapper.getInfo());
        startPoint.countDown();
        for (Stresser stresser : stressers) {
            stresser.join();
            log.info("stresser[" + stresser.getName() + "] finsihed");
        }
        log.info("****BARRIER JOIN PASSED****");

        for(Stresser s : stressers) {
            pooledKeys.addAll(s.pooledKeys);
        }

        for(String key : pooledKeys) {
            if(cacheWrapper.isKeyLocal(key)) {
                finalValues.put(key, cacheWrapper.get("", key));
            }
        }

        log.info("****KEYS OBTAINED****");

        Thread.sleep(30000);

        return stressers;
    }

    private class Stresser extends Thread {

        private ArrayList<String> pooledKeys = new ArrayList<String>(numberOfKeys);

        private int threadIndex;
        private String bucketId;
        private int nrFailures;
        private int nrFailsAtCommit = 0; //*******
        private long readDuration = 0;
        private long writeDuration = 0;
        private long reads;
        private long writes;
        private long startTime;
        private AtomicInteger requestsLeft;


        private long delta=0;
        private int numWriteSkew=0;
        private int numTmeout=0;
        private int numLocalTimeOut=0;
        private int numRemoteTimeOut=0;

        private ExecutorService logger;


        //Added to support primary-backup scenario's simulation
        private int lower_bound_tx_op;
        private int upper_bound_tx_op;
        private boolean is_master_thread;

        private long commitSuccessDuration = 0;
        private long commitFailDuration = 0;
        private long commitTotalDuration = 0;
        private long simulTime;

        private double sum_begin=0;
        private double temp_begin=0;
        private double temp_write=0;
        private double sum_write=0;
        private double tsend= 0.22; //msec
        private long good_write=0;
        private long num_tx=0;
        private int numDlD = 0;

        public Stresser(int threadIndex, AtomicInteger requestsLeft, boolean is_master_thread, int lb, int ub, long time) {
            super("Stresser-" + threadIndex);
            this.threadIndex = threadIndex;
            bucketId = getBucketId(threadIndex);
            this.requestsLeft = requestsLeft;

            this.is_master_thread=is_master_thread;
            this.lower_bound_tx_op=lb;
            this.upper_bound_tx_op=ub;
            this.simulTime=time;
        }

        @Override
        public void run() {
            Random r = new Random();
            int randomKeyInt;
            try {
                startPoint.await();
                log.info("Starting thread: " + getName());
            } catch (InterruptedException e) {
                log.warn(e);
            }

            int i = 0;
            int op_left = 0;
            boolean successful;
            long start = 0;

            long init_time=System.nanoTime();
            long commit_start=0;
            boolean already_written;

            if(coordinatorParticipation || !cacheWrapper.isPrimary()) {
                while(delta<this.simulTime){

                    successful=true;
                    already_written=false;
                    op_left=opPerTx(this.lower_bound_tx_op,this.upper_bound_tx_op,r);

                    start = System.currentTimeMillis();
                    temp_begin=System.nanoTime();

                    cacheWrapper.startTransaction();
                    log.info("*** [" + getName() + "] new transaction: " + i + "***");

                    while(op_left > 0 && successful){
                        randomKeyInt = r.nextInt(numberOfKeys - 1);
                        String key = getKey(randomKeyInt);

                        Object result = null;

                        if (r.nextInt(100)>=writePercentage && !(op_left==1 && !already_written)) {
                            try {
                                result = cacheWrapper.get(bucketId, key);
                            } catch (Throwable e) {
                                log.info(this.threadIndex+" Errore nella get");
                                log.warn(e);
                                successful=false;
                            }
                        }
                        else {
                            String payload = generateRandomString(sizeOfValue);

                            try {
                                already_written=true;
                                temp_write=System.nanoTime();
                                cacheWrapper.put(bucketId, key, payload);
                                sum_write+=(System.nanoTime()-temp_write);
                                good_write++;

                            } catch (Throwable e) {
                                if(e.getClass().getName().equals("org.infinispan.CacheException"))
                                    this.numWriteSkew++;
                                if(e.getClass().getName().equals("org.infinispan.util.concurrent.TimeoutException"))
                                    this.numLocalTimeOut++;
                                if(e.getClass().getName().equals("org.infinispan.util.concurrent.locks.DeadlockDetectedException"))
                                    this.numDlD++;
                                successful=false;
                                log.info(this.threadIndex+" Errore nella put");
                                log.warn(e);
                            }

                        }
                        op_left--;
                        //logProgress(i, result);
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
                    catch(Throwable rb){
                        log.info(this.threadIndex+" errore_nella_commit");
                        nrFailures++;
                        nrFailsAtCommit++;
                        successful=false;
                        log.warn(rb);

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

                    if(already_written){
                        writeDuration += System.currentTimeMillis() - start;
                        if(successful) {
                            writes++;
                        }
                    }
                    else{    //ro transaction
                        readDuration += System.currentTimeMillis() - start;
                        if(successful) {
                            reads++;
                        }
                    }
                    this.delta = System.nanoTime() - init_time;
                }
            } else {
                long sleepTime = this.simulTime/1000000;//nano to millis
                System.out.println("I am a coordinator and I wouldn't execute transactions. sleep for "
                        + sleepTime + "(ms)");
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {}
            }

            try {
                Thread.sleep(30000); //30 seconds
            } catch (InterruptedException e) {}
        }


        private void logProgress(int i, Object result) {
            if ((i + 1) % opsCountStatusLog == 0) {
                double elapsedTime = System.currentTimeMillis() - startTime;
                double estimatedTotal = ((double) (numberOfRequests / numOfThreads) / (double) i) * elapsedTime;
                double estimatedRemaining = estimatedTotal - elapsedTime;
                if (log.isTraceEnabled()) {
                    log.trace("i=" + i + ", elapsedTime=" + elapsedTime);
                }
                log.info("Thread index '" + threadIndex + "' executed " + (i + 1) + " operations. Elapsed time: " +
                        Utils.getDurationString((long) elapsedTime) + ". Estimated remaining: " + Utils.getDurationString((long) estimatedRemaining) +
                        ". Estimated total: " + Utils.getDurationString((long) estimatedTotal));
                System.out.println("Last result" + result);//this is printed here just to make sure JIT doesn't
                // skip the call to cacheWrapper.get
            }
        }

        public long totalDuration() {
            return readDuration + writeDuration;
        }

        public long delta(){
            return this.delta;
        }



        /*Popoliamo soltanto l'insieme pooledkeys: le prime put saranno delle VERE put e NON
delle replace come nel caso del benchmark standard (come indicato sul sito)
questo per evitare che qualcuno abbia dati piu' "freschi" in cache di altri  */

        //
        public void initializeKeys() {
            for (int keyIndex = 0; keyIndex < numberOfKeys; keyIndex++) {
                String key="key:"+keyIndex;
                pooledKeys.add(key);
                /*
                        cacheWrapper.put(this.bucketId, key, generateRandomString(sizeOfValue));

                */
            }
        }

        public String getKey(int keyIndex) {
            return pooledKeys.get(keyIndex);
        }
    }

    private String str(Object o) {
        return String.valueOf(o);
    }

    public void setNumberOfRequests(int numberOfRequests) {
        this.numberOfRequests = numberOfRequests;
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

    public String getBucketPrefix() {
        return bucketPrefix;
    }

    public void setBucketPrefix(String bucketPrefix) {
        this.bucketPrefix = bucketPrefix;
    }

    @Override
    public String toString() {
        return "PutGetStressor{" +
                "opsCountStatusLog=" + opsCountStatusLog +
                ", numberOfRequests=" + numberOfRequests +
                ", numberOfKeys=" + numberOfKeys +
                ", sizeOfValue=" + sizeOfValue +
                ", writePercentage=" + writePercentage +
                ", numOfThreads=" + numOfThreads +
                ", bucketPrefix=" + bucketPrefix +
                ", cacheWrapper=" + cacheWrapper +
                "}";
    }

    private int opPerTx(int lower_bound, int upper_bound,Random ran){
        if(lower_bound==upper_bound)
            return lower_bound;
        return(ran.nextInt(upper_bound-lower_bound)+lower_bound);


    }

    class eventWriter implements Runnable {
        private String event;

        public eventWriter(String s) {
            event=s;
        }

        public void run() {


            log.info(event);

        }
    }

    public void setLowerBoundOp(int lower_bound_op) {
        this.lower_bound_op = lower_bound_op;
    }

    public void setUpperBoundOp(int upper_bound_op) {
        this.upper_bound_op = upper_bound_op;
    }

    public void setPerThreadSimulTime(long simulTime) {
        this.simulTime = simulTime;
    }
}

