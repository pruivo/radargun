package org.radargun.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 21/05/12
 */
public class MemoryStat {

   private MemoryMXBean memoryBean;

   public MemoryStat(){
      this.memoryBean = ManagementFactory.getMemoryMXBean();
   }

   public long getUsedMemory(){
      return this.memoryBean.getHeapMemoryUsage().getUsed();
   }

   public long getCommittedMemory(){
      return this.memoryBean.getHeapMemoryUsage().getCommitted();
   }

}
