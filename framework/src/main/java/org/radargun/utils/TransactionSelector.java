package org.radargun.utils;

import org.radargun.CacheWrapper;

import java.util.Random;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class TransactionSelector {

   private int lowerBoundWriteOperationWriteTx;
   private int upperBoundWriteOperationWriteTx;

   private int lowerBoundReadOperationWriteTx;
   private int upperBoundReadOperationWriteTx;

   private int lowerBoundOperationReadTx;
   private int upperBoundOperationReadTx;

   private int writeTxPercentage;   

   private final Random random;

   public TransactionSelector(Random random) {
      this.random = random;
   }

   public synchronized final OperationIterator chooseTransaction(CacheWrapper cacheWrapper) {
      boolean readOnly = cacheWrapper.canExecuteReadOnlyTransactions() &&
            (!cacheWrapper.canExecuteWriteTransactions() || random.nextInt(100) >= writeTxPercentage);

      if (readOnly) {
         return new OperationIterator(true, random, chooseOperations(lowerBoundOperationReadTx, upperBoundOperationReadTx), 0);
      } else {
         return new OperationIterator(false, random, 
                                      chooseOperations(lowerBoundReadOperationWriteTx, upperBoundReadOperationWriteTx),
                                      chooseOperations(lowerBoundWriteOperationWriteTx, upperBoundWriteOperationWriteTx));
      }
   }

   public synchronized final void setWriteOperationsForWriteTx(String ops) {
      int[] numbers = parseString(ops);
      if (numbers != null) {
         lowerBoundWriteOperationWriteTx = numbers[0];
         upperBoundWriteOperationWriteTx = numbers[1];
      }
   }

   public synchronized final void setReadOperationsForWriteTx(String ops) {
      int[] numbers = parseString(ops);
      if (numbers != null) {
         lowerBoundReadOperationWriteTx = numbers[0];
         upperBoundReadOperationWriteTx = numbers[1];
      }
   }

   public synchronized final void setReadOperationsForReadTx(String ops) {
      int[] numbers = parseString(ops);
      if (numbers != null) {
         lowerBoundOperationReadTx = numbers[0];
         upperBoundOperationReadTx = numbers[1];
      }
   }

   public synchronized final void setWriteTxPercentage(int writeTxPercentage) {
      this.writeTxPercentage = writeTxPercentage;
   }

   private int[] parseString(String string) {
      String[] split = string.split(":", 2);
      if (split.length == 1) {
         int val = parseInt(split[0]);
         if (val == -1) {
            return null;
         } else {
            return new int[] {val, val};
         }
      }

      int val = parseInt(split[0]);
      int val1 = parseInt(split[1]);

      if (val != -1 && val1 != -1) {
         return val > val1 ? new int[] {val1, val} : new int[] {val, val1};
      } else if (val == -1 && val1 == -1) {
         return null;
      } else if (val == -1) {
         return new int[] {val1, val1};
      } else {
         return new int[] {val, val};
      }
   }

   private int parseInt(String number) {
      try {
         int val =  Integer.parseInt(number);
         return val <= 0 ? -1 : val;
      } catch (Exception e) {
         return -1;               
      }
   }

   private int chooseOperations(int lowerBound, int upperBound) {
      if(lowerBound == upperBound)
         return lowerBound;
      return(random.nextInt(upperBound - lowerBound) + lowerBound);
   }

   public static class OperationIterator {
      private final boolean isReadOnly;
      private final Random random;      

      private int writeOperations;
      private int readOperations;

      private OperationIterator(boolean readOnly, Random random, int readOperations,
                                int writeOperations) {
         isReadOnly = readOnly;
         this.random = random;         
         this.writeOperations = writeOperations;
         this.readOperations = readOperations;
         if (!readOnly && writeOperations == 0) {
            this.writeOperations = 1;
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

         if (random.nextInt(100) >= 50) {
            readOperations--;
            return true;
         } else {
            writeOperations--;
            return false;
         }
      }

      public final boolean isReadOnly() {
         return isReadOnly;
      }

   }
}
