package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.BankWarmupStressor;

import java.util.List;

/**
 * This stage shuld be run before the actual test, in order to activate JIT compiler. It will perform
 * <b>operationCount</b> puts and then gets on the cache wrapper. Note: this stage won't clear the added data from
 * slave.
 * <pre>
 * Params:
 *       - operationCount : the number of operations to perform.
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 */
public class BankWarmupStage extends AbstractDistStage {

    private int numberOfAccounts = 10;
    private int initialAmmount = 1000;

    public DistStageAck executeOnSlave() {
        DefaultDistStageAck ack = newDefaultStageAck();
        CacheWrapper wrapper = slaveState.getCacheWrapper();
        if (wrapper == null) {
            log.info("Not executing any test as the wrapper is not set up on this slave ");
            return ack;
        }
        long startTime = System.currentTimeMillis();
        warmup(wrapper);
        long duration = System.currentTimeMillis() - startTime;
        log.info("The warmup took: " + (duration / 1000) + " seconds.");
        ack.setPayload(duration);
        return ack;
    }

    private void warmup(CacheWrapper wrapper) {
        BankWarmupStressor warmup = new BankWarmupStressor(getSlaveIndex(), getActiveSlaveCount());
        warmup.setNumberOfAccounts(numberOfAccounts);
        warmup.setInitialAmmount(initialAmmount);
        warmup.stress(wrapper);
    }

    public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
        logDurationInfo(acks);
        for (DistStageAck ack : acks) {
            DefaultDistStageAck dAck = (DefaultDistStageAck) ack;
            if (log.isTraceEnabled()) {
                log.trace("Warmup on slave " + dAck.getSlaveIndex() + " finished in " + dAck.getPayload() + " millis.");
            }
        }
        return true;
    }

    public void setNumberOfAccounts(int numberOfAccounts) {
        this.numberOfAccounts = numberOfAccounts;
    }

    public void setInitialAmmount(int initialAmmount) {
        this.initialAmmount = initialAmmount;
    }

    @Override
    public String toString() {
        return "WarmupStage {" +
                "numberOfAccounts=" + numberOfAccounts + ", " +
                "initialAmmount=" + initialAmmount + ", " + super.toString();
    }
}
