package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.stressors.AbstractCacheWrapperStressor;
import org.radargun.stressors.BankStressor;

/** 
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class PrintDataContainerStage extends AbstractDistStage {


   @Override
   public DistStageAck executeOnSlave() {
      DefaultDistStageAck ack = newDefaultStageAck();
      CacheWrapper cacheWrapper = slaveState.getCacheWrapper();
      
      if (cacheWrapper == null) {
         log.fatal("Cannot print data container in a null cache wrapper");         
         return ack;
      }
            
      String dataContainer = cacheWrapper.dataContainerToString();
      
      log.info("\n===========\n");
      log.info(dataContainer);
      log.info("\n===========\n");
      
      return ack;
   }
}
