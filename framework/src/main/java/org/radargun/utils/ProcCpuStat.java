package org.radargun.utils;

import java.io.IOException;

/**
 * Author: Diego Didona
 * Email: didona@gsd.inesc-id.pt
 * Websiste: www.cloudtm.eu
 * Date: 21/05/12
 */
public class ProcCpuStat implements CpuStat{

   static final int USER = 0;
   static final int NICE = 1;
   static final int SYSTEM = 2;
   static final int IDLE = 3;

   private long[] cpuTimes;


   public ProcCpuStat() {
      this.cpuTimes = this.parseCpuTime();
   }


   /*
   We could use the same approach Galder uses in Radargun, which is much more elegant and, I think, portable
   This approach involves explicit command-line invocations and relies on the presence of the proc fs
    */


   private long[] parseCpuTime() {
      Runtime rt = Runtime.getRuntime();
      String[] cmd = {"cat", "/proc/stat"};
      String[] temp;
      long[] ret = new long[4];
      try {
         Process p = rt.exec(cmd);
         java.io.BufferedReader stdInput = new java.io.BufferedReader(new java.io.InputStreamReader(p.getInputStream()));
         String actual = stdInput.readLine();

         temp = actual.split(" ");

         //The output has two spaces after the first token!!!
         for (int i = 2; i < 6; i++) {
            ret[i - 2] = Long.parseLong(temp[i]);
         }
      } catch (IOException ioe) {
         ioe.printStackTrace();
         System.exit(-1);
      }
      return ret;
   }

   public double getCpuUsage() {
      long[] current = this.parseCpuTime();
      return this.getCpuUsage(current);
   }

   public double getCpuUsageAndReset(){
      double ret = this.getCpuUsage();
      this.reset();
      return ret;
   }

   private double getCpuUsage(long[] usages) {

      long[] temp = new long[4];


      //obtain usages relevant to last monitoring window
      for (int i = 0; i < 4; i++) {
         temp[i] = usages[i] - this.cpuTimes[i];
      }

      double cpuUsage = (temp[USER] + temp[NICE] + temp[SYSTEM]);
      double totalCpuTime = cpuUsage + temp[IDLE];

      return cpuUsage / totalCpuTime;

   }



   public void reset(){
      this.cpuTimes = this.parseCpuTime();
   }



}
