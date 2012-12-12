package org.radargun.workloads;

import org.radargun.CacheWrapper;

import java.util.Random;

/**
 * The transaction workload factory for the Synthetic Benchmark
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class SyntheticTransactionWorkloadFactory extends TransactionWorkloadFactory<SyntheticTransactionWorkload> {

   private int[] writeTransactionReads;
   private int[] writeTransactionsWrites;
   private int[] readTransactionsReads;
   private int writeTransactionPercentage;
   private volatile CurrentWorkload currentWorkload;

   @Override
   public final SyntheticTransactionWorkload chooseTransaction(Random random, CacheWrapper wrapper) {
      CurrentWorkload workload = currentWorkload;
      if (workload.isNextReadOnly(wrapper.isPassiveReplication(), wrapper.isTheMaster(), random)) {
         return new SyntheticTransactionWorkload(true, random, 0, workload.selectReadOperations(true, random));
      } else {
         return new SyntheticTransactionWorkload(false, random, workload.selectWriteOperations(random),
                                                 workload.selectReadOperations(false, random));
      }
   }

   @Override
   public final void calculate() {
      currentWorkload = new CurrentWorkload(writeTransactionReads, writeTransactionsWrites, readTransactionsReads,
                                            writeTransactionPercentage);
   }

   public final void setWriteTransactionReads(String reads) {
      int[] temp = parse(reads);
      if (!isValid(temp)) {
         return;
      }
      writeTransactionReads = temp;
   }

   public final void setWriteTransactionwrites(String writes) {
      int[] temp = parse(writes);
      if (!isValid(temp)) {
         return;
      }
      writeTransactionsWrites = temp;
   }

   public final void setReadTransactionsReads(String reads) {
      int[] temp = parse(reads);
      if (!isValid(temp)) {
         return;
      }
      readTransactionsReads = temp;
   }

   public final void setWriteTransactionPercentage(int writeTransactionPercentage) {
      this.writeTransactionPercentage = writeTransactionPercentage;
   }

   private class CurrentWorkload {
      private final int[] writeTransactionReads;
      private final int[] writeTransactionsWrites;
      private final int[] readTransactionsReads;
      private final int writeTransactionPercentage;

      private CurrentWorkload(int[] writeTransactionReads, int[] writeTransactionsWrites, int[] readTransactionsReads, int writeTransactionPercentage) {
         this.writeTransactionReads = writeTransactionReads;
         this.writeTransactionsWrites = writeTransactionsWrites;
         this.readTransactionsReads = readTransactionsReads;
         this.writeTransactionPercentage = writeTransactionPercentage;
      }

      public final boolean isNextReadOnly(boolean passiveReplication, boolean master, Random random) {
         return (passiveReplication && !master) || random.nextInt(100) >= writeTransactionPercentage;
      }

      public final int selectReadOperations(boolean readOnly, Random random) {
         return readOnly ? operations(readTransactionsReads, random) : operations(writeTransactionReads, random);
      }

      public final int selectWriteOperations(Random random) {
         return operations(writeTransactionsWrites, random);
      }

      private int operations(int[] lowerAndUpperBound, Random random) {
         if (lowerAndUpperBound.length == 1) {
            return lowerAndUpperBound[0];
         }
         return random.nextInt(lowerAndUpperBound[1] - lowerAndUpperBound[0] + 1) + lowerAndUpperBound[0];
      }
   }
}
