package org.radargun.stages;

import org.radargun.DistStageAck;
import org.radargun.state.MasterState;

import java.util.List;

/**
 * Date: 1/18/12
 * Time: 6:19 PM
 *
 * @author pruivo
 */
public class GetKeysStage extends AbstractDistStage {
    //TODO receive all the keys and check if they are the same or not

    @Override
    public DistStageAck executeOnSlave() {
        return null;
    }

    @Override
    public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
        return true;
    }

}
