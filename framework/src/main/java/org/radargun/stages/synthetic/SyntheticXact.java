package org.radargun.stages.synthetic;

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
   XactOp[] ops;

   public SyntheticXact() {
      initResponseTime = System.nanoTime();
      initServiceTime = initResponseTime;
   }


   public SyntheticXact(SyntheticXact ex, boolean sameXact) {
      initServiceTime = System.nanoTime();
      initResponseTime = sameXact? ex.getInitResponseTime():initServiceTime;
      ops = ex.getOps();
      clazz = ex.getClazz();
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
}