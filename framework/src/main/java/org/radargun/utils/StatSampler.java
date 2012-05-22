package org.radargun.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 21/05/12
 */
public class StatSampler extends TimerTask {

   private CpuStat CpuStat;
   private MemoryStat memoryStat;
   private Timer timer;

   private LinkedList<Long> usedMemories = new LinkedList<Long>();
   private LinkedList<Double> usedCpu = new LinkedList<Double>();

   private static final Log log = LogFactory.getLog(StatSampler.class);


   public StatSampler() throws IOException{
      init();
      this.timer.schedule(this,5000L);
   }

   public StatSampler(long interval) throws IOException{
      init();
      this.timer.schedule(this,interval,interval);
   }

   private void init() throws IOException {

      this.CpuStat = new ProcCpuStat();
      this.memoryStat = new MemoryStat();
      this.timer = new Timer();
   }

   public void run(){
      this.usedMemories.addLast(memoryStat.getUsedMemory());
      this.usedCpu.addLast(CpuStat.getCpuUsageAndReset());
   }

   public LinkedList<Long> getMemoryUsageHistory(){
      return this.usedMemories;
   }
   public LinkedList<Double> getCpuUsageHistory(){
      return this.usedCpu;
   }

}
