package org.radargun.stages.synthetic;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticXact extends Xact{
   private Object[] readSet;
   private Object[] writeSet;
   private long initResponseTime;
   private long initServiceTime;
   public xactClass clazz;
   private boolean isCommit;  //we track only commit-abort without considering also xact that can abort because of application logic (and might be not restarted, then)


   private class xactOp {
      private Object key;
      private Object value;
      private boolean isPut;
   }


   public Object[] getReadSet() {
      return readSet;
   }

   public void setReadSet(Object[] readSet) {
      this.readSet = readSet;
   }

   public Object[] getWriteSet() {
      return writeSet;
   }

   public void setWriteSet(Object[] writeSet) {
      this.writeSet = writeSet;
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