package org.radargun.workloads;

import java.util.Random;

/**
 * Represents a transaction in the Synthetic Benchmark
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class SyntheticTransactionWorkload extends AbstractTransactionWorkload {

   private int writeOperations;
   private int readOperations;

   public SyntheticTransactionWorkload(boolean readOnly, Random threadRandomGenerator, int writeOperations, int readOperations) {
      super(readOnly, threadRandomGenerator);
      this.readOperations = readOperations;
      if (!readOnly && writeOperations == 0) {
         this.writeOperations = 1;
      } else {
         this.writeOperations = writeOperations;
      }
   }

   public final boolean hasNext() {
      return writeOperations > 0 || readOperations > 0;
   }

   public final boolean isNextOperationARead() {
      boolean canBeARead = readOperations > 0;
      boolean canBeAWrite = writeOperations > 0;

      if (canBeARead && !canBeAWrite) {
         readOperations--;
         return true;
      } else if (!canBeARead && canBeAWrite) {
         writeOperations--;
         return false;
      }

      if (getThreadRandomGenerator().nextInt(100) >= 50) {
         readOperations--;
         return true;
      } else {
         writeOperations--;
         return false;
      }
   }
}
