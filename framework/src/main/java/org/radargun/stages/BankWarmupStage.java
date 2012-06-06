package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.BankWarmupStressor;

import java.util.List;

/**
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class BankWarmupStage extends AbstractDistStage {

    private int numberOfAccounts = 10;
    private int initialAmount = 1000;

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
        warmup.setInitialAmmount(initialAmount);
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

    public void setInitialAmount(int initialAmount) {
        this.initialAmount = initialAmount;
    }

    @Override
    public String toString() {
        return "BankWarmupStage {" +
                "numberOfAccounts=" + numberOfAccounts + ", " +
                "initialAmount=" + initialAmount + ", " + super.toString();
    }
}
