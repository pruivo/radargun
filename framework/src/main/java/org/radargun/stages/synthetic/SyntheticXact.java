package org.radargun.stages.synthetic;

import org.radargun.CacheWrapper;

import java.util.Arrays;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticXact extends Xact {

   private long initResponseTime;
   private long initServiceTime;
   public xactClass clazz;
   private boolean isCommit;  //we track only commit-abort without considering also xact that can abort because of application logic (and might be not restarted, then)
   private CacheWrapper cache;
   XactOp[] ops;


   public SyntheticXact(CacheWrapper wrapper) {
      initResponseTime = System.nanoTime();
      initServiceTime = initResponseTime;
      cache = wrapper;
   }


   public CacheWrapper getCache() {
      return cache;
   }

   public void setCache(CacheWrapper wrapper) {
      this.cache = wrapper;
   }

   public XactOp[] getOps() {
      return ops;
   }

   public void setOps(XactOp[] ops) {
      this.ops = ops;
   }

   public long getInitResponseTime() {
      return initResponseTime;
   }

   public void setInitResponseTime(long initResponseTime) {
      this.initResponseTime = initResponseTime;
   }

   public long getInitServiceTime() {
      return initServiceTime;
   }

   public void setInitServiceTime(long initServiceTime) {
      this.initServiceTime = initServiceTime;
   }

   public xactClass getClazz() {
      return clazz;
   }

   public void setClazz(xactClass clazz) {
      this.clazz = clazz;
   }

   public boolean isCommit() {
      return isCommit;
   }

   public void setCommit(boolean commit) {
      isCommit = commit;
   }

   public void executeLocally() throws Exception {
      for(XactOp op:ops){
         if(op.isPut())
            cache.put(null,op.getKey(),op.getValue());
         else
            cache.get(null,op.getKey());
      }
   }

   @Override
   public String toString() {
      return "SyntheticXact{" +
            "initResponseTime=" + initResponseTime +
            ", initServiceTime=" + initServiceTime +
            ", clazz=" + clazz +
            ", isCommit=" + isCommit +
            ", cache=" + cache +
            ", ops=" + Arrays.toString(ops) +
            '}';
   }
}