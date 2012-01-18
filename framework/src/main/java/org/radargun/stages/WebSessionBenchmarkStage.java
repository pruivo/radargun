package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.PutGetStressor;

import java.io.BufferedWriter;
import java.io.FileWriter;
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

    public static final String SESSION_PREFIX = "SESSION";

    //for each session there will be created fixed number of attributes. On those attributes all the GETs and PUTs are
    // performed (for PUT is overwrite)
    private int numberOfAttributes = 10000;

    //Each attribute will be a byte[] of this size
    private int sizeOfAnAttribute = 1000;

    //Out of the total number of request, this define the frequency of writes (percentage)
    private int writePercentage = 20;

    //the number of threads that will work on this slave
    private int numOfThreads = 10;

    //indicates that the coordinator executes transactions or not
    private boolean coordinatorParticipation = true;

    //needed to compute the uniform distribution of operations per transaction
    private int lowerBoundOp = 100;

    // as above
    private int upperBoundOp = 100;

    //simulation time
    private long perThreadSimulTime;

    //allows read only transaction
    private boolean readOnlyTransactionsEnabled = false;

    //allows execution without contention
    private boolean noContentionEnabled = false;

    private CacheWrapper cacheWrapper;
    private int opsCountStatusLog = 5000;
    private boolean reportNanos = false;


    public DistStageAck executeOnSlave() {
        DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
        this.cacheWrapper = slaveState.getCacheWrapper();
        if (cacheWrapper == null) {
            log.info("Not running test on this slave as the wrapper hasn't been configured.");
            return result;
        }

        log.info("Starting WebSessionBenchmarkStage: " + this.toString());

        PutGetStressor putGetStressor = new PutGetStressor();
        putGetStressor.setSlaveIdx(getSlaveIndex());
        putGetStressor.setLowerBoundOp(lowerBoundOp);
        putGetStressor.setUpperBoundOp(upperBoundOp);
        putGetStressor.setSimulationTime(perThreadSimulTime);
        putGetStressor.setBucketPrefix(getSlaveIndex() + "");
        putGetStressor.setNumberOfAttributes(numberOfAttributes);
        putGetStressor.setNumOfThreads(numOfThreads);
        putGetStressor.setOpsCountStatusLog(opsCountStatusLog);
        putGetStressor.setSizeOfAnAttribute(sizeOfAnAttribute);
        putGetStressor.setWritePercentage(writePercentage);
        putGetStressor.setCoordinatorParticipation(coordinatorParticipation);
        putGetStressor.setReadOnlyTransactionsEnabled(readOnlyTransactionsEnabled);
        putGetStressor.setNoContentionEnabled(noContentionEnabled);

        try {
            Map<String, String> results = putGetStressor.stress(cacheWrapper);
            result.setPayload(results);
            /*try {
                BufferedWriter bw = new BufferedWriter(new FileWriter("./keys" + System.nanoTime()));
                for(Map.Entry<String,Object> entry : putGetStressor.getAllKeys().entrySet()) {
                    bw.write("*** " + entry.getKey() + " => " + entry.getValue());
                    bw.newLine();
                }
                bw.flush();
                bw.close();
            } catch(Exception e) {
                log.warn("++++++++++ ups... +++++++++");
            }*/

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

    @Override
    public String toString() {
        return "WebSessionBenchmarkStage {" +
                "opsCountStatusLog=" + opsCountStatusLog +
                ", numberOfAttributes=" + numberOfAttributes +
                ", sizeOfAnAttribute=" + sizeOfAnAttribute +
                ", writePercentage=" + writePercentage +
                ", numOfThreads=" + numOfThreads +
                ", reportNanos=" + reportNanos +
                ", coordinatorParticipation=" + coordinatorParticipation +
                ", lowerBoundOp=" + lowerBoundOp +
                ", upperBoundOp=" + upperBoundOp +
                ", perThreadSimulTime=" + perThreadSimulTime +
                ", readOnlyTransactionsEnabled=" + readOnlyTransactionsEnabled +
                ", noContentionEnabled=" + noContentionEnabled +
                ", cacheWrapper=" + cacheWrapper +
                ", " + super.toString();
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

    public void setReadOnlyTransactionsEnabled(boolean readOnlyTransactionsEnabled) {
        this.readOnlyTransactionsEnabled = readOnlyTransactionsEnabled;
    }

    public void setNoContentionEnabled(boolean noContentionEnabled) {
        this.noContentionEnabled = noContentionEnabled;
    }
}
