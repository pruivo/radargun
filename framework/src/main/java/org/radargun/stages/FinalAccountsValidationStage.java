package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.state.MasterState;
import org.radargun.stressors.FinalAccountsValidationStressor;

import java.util.List;
import java.util.Map;

/**
 * @author pedro
 *         Date: 05-08-2011
 */
public class FinalAccountsValidationStage extends AbstractDistStage {

    //process only on slave index 0
    public DistStageAck executeOnSlave() {
        DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());

        if(getSlaveIndex() == 0) {
            CacheWrapper cacheWrapper = slaveState.getCacheWrapper();
            FinalAccountsValidationStressor stressor = new FinalAccountsValidationStressor();
            Map<String, String> results = stressor.stress(cacheWrapper);
            result.setPayload(results);
        }
        return result;
    }

    @Override
    public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
        logDurationInfo(acks);
        for (DistStageAck ack : acks) {
            DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
            if (wAck.isError()) {
                log.warn("Received error ack: " + wAck);
            } else if(wAck.getSlaveIndex() == 0) {
                Map<String, String> benchResult = (Map<String, String>) wAck.getPayload();
                log.info(" +++ validation result received: " + benchResult + " +++ ");
            }
        }
        return true;
    }


}
