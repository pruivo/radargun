package org.radargun.workloads;

import java.util.Random;

/**
 * Represents a transaction to be executed
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public abstract class AbstractTransactionWorkload {

   private final boolean isReadOnly;
   private final Random threadRandomGenerator;
   private long startExecutionTimestamp;
   private long endExecutionTimestamp;
   private long endCommitTimestamp;
   private boolean executionOK;
   private boolean commitOK;

   protected AbstractTransactionWorkload(boolean readOnly, Random threadRandomGenerator) {
      isReadOnly = readOnly;
      this.threadRandomGenerator = threadRandomGenerator;
      this.executionOK = true;
      this.commitOK = true;
   }

   public final boolean isReadOnly() {
      return isReadOnly;
   }

   public final void setStartExecutionTimestamp() {
      this.startExecutionTimestamp = now();
   }

   public final void setEndExecutionTimestamp() {
      this.endExecutionTimestamp = now();
   }

   public final void setEndCommitTimestamp() {
      this.endCommitTimestamp = now();
   }

   public final void markExecutionFailed() {
      this.executionOK = false;
   }

   public final void markCommitFailed() {
      this.commitOK = false;
   }

   public final boolean isExecutionOK() {
      return executionOK;
   }

   public final boolean isCommitOK() {
      return commitOK;
   }

   public final long getExecutionDuration() {
      return endExecutionTimestamp - startExecutionTimestamp;
   }

   public final long getCommitDuration() {
      return endCommitTimestamp - endExecutionTimestamp;
   }

   public final Random getThreadRandomGenerator() {
      return threadRandomGenerator;
   }

   protected final long now() {
      return System.nanoTime();
   }
}
