package org.radargun.utils;

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

   private CpuStat cpuStat;
   private MemoryStat memoryStat;
   private Timer timer;

   private LinkedList<Long> usedMemories = new LinkedList<Long>();
   private LinkedList<Double> usedCpu = new LinkedList<Double>();


   public StatSampler(){
      init();
   }

   public StatSampler(long interval){
      init();
      this.timer.schedule(this,interval);
   }

   private void init(){
      this.cpuStat = new CpuStat();
      this.memoryStat = new MemoryStat();
      this.timer = new Timer();
   }

   public void run(){
      this.usedMemories.addLast(memoryStat.getUsedMemory());
      this.usedCpu.addLast(cpuStat.getCpuUsageAndReset());
   }

   public LinkedList<Long> getMemoryUsageHistory(){
      return this.usedMemories;
   }
   public LinkedList<Double> getCpuUsageHistory(){
      return this.usedCpu;
   }

}
