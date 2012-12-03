package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;

/**
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class PrintDataContainerStage extends AbstractDistStage {

   private String commitLogPrefix = null;
   private String dataContainerPrefix = null;

   @Override
   public DistStageAck executeOnSlave() {
      DefaultDistStageAck ack = newDefaultStageAck();
      CacheWrapper cacheWrapper = slaveState.getCacheWrapper();

      if (cacheWrapper == null) {
         log.fatal("Cannot print data container in a null cache wrapper");
         return ack;
      }

      if (dataContainerPrefix != null) {
         String filePath = getFilePath(dataContainerPrefix);
         log.info("Dumping data container to " + filePath);
         cacheWrapper.dumpDataContainer(filePath);
         log.info("Dump of data container finished!");
      } else {
         log.info("No dump of data container");
      }

      if (commitLogPrefix != null) {
         String filePath = getFilePath(commitLogPrefix);
         log.info("Dumping commit log to " + filePath);
         cacheWrapper.dumpCommitLog(filePath);
         log.info("Dump of commit log finished!");
      } else {
         log.info("No dump of commit log");
      }

      return ack;
   }

   public void setCommitLogPrefix(String commitLogPrefix) {
      this.commitLogPrefix = commitLogPrefix;
   }

   public void setDataContainerPrefix(String dataContainerPrefix) {
      this.dataContainerPrefix = dataContainerPrefix;
   }

   private String getFilePath(String prefix) {
      return prefix + "-" + slaveIndex + ".txt";
   }
}
