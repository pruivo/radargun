package org.radargun.workloads;

import org.radargun.CacheWrapper;

import java.util.Random;

/**
 * The Bank Benchmark transaction workload factory
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class BankTransactionWorkloadFactory extends TransactionWorkloadFactory<BankTransactionWorkload> {

   private int transferWeight;
   private int depositWeight;
   private int withdrawWeight;
   private int[] numberOfTransfers;
   private int[] numberOfDeposits;
   private int[] numberOfWithdraws;
   private volatile CurrentWorkload currentWorkload;

   @Override
   public BankTransactionWorkload chooseTransaction(Random random, CacheWrapper wrapper) {
      int nextOp = random.nextInt(100);
      CurrentWorkload workload = currentWorkload;
      if(nextOp < workload.transferWeight) {
         //TODO
         return new BankTransactionWorkload(false, random, workload.selectTransfer(random), TransactionType.TRANSFER);
      } else if (nextOp < (workload.transferWeight + workload.depositWeight)) {
         return new BankTransactionWorkload(false, random, workload.selectDeposits(random), TransactionType.DEPOSIT);
      } else if (nextOp < (workload.transferWeight + workload.depositWeight + workload.withdrawWeight)) {
         return new BankTransactionWorkload(false, random, workload.selectWithdraws(random), TransactionType.WITHDRAW);
      } else {
         return new BankTransactionWorkload(true, random, -1, TransactionType.CHECK_ALL_ACCOUNTS);
      }
   }

   public final void setTransferWeight(int transferWeight) {
      this.transferWeight = transferWeight;
   }

   public final void setDepositWeight(int depositWeight) {
      this.depositWeight = depositWeight;
   }

   public final void setWithdrawWeight(int withdrawWeight) {
      this.withdrawWeight = withdrawWeight;
   }

   public final void setNumberOfTransfers(String transfers) {
      int[] temp = parse(transfers);
      if (!isValid(temp)) {
         return;
      }
      numberOfTransfers = temp;
   }

   public final void setNumberOfDeposits(String deposits) {
      int[] temp = parse(deposits);
      if (!isValid(temp)) {
         return;
      }
      numberOfDeposits = temp;
   }

   public final void setNumberOfWithdraws(String withdraws) {
      int[] temp = parse(withdraws);
      if (!isValid(temp)) {
         return;
      }
      numberOfWithdraws = temp;
   }

   @Override
   public final void calculate() {
      currentWorkload = new CurrentWorkload(transferWeight, depositWeight, withdrawWeight, numberOfTransfers,
                                            numberOfDeposits, numberOfWithdraws);
   }

   public static enum TransactionType {
      CHECK_ALL_ACCOUNTS,
      TRANSFER,
      DEPOSIT,
      WITHDRAW
   }

   private class CurrentWorkload {
      private final int transferWeight;
      private final int depositWeight;
      private final int withdrawWeight;
      private final int[] numberOfTransfers;
      private final int[] numberOfDeposits;
      private final int[] numberOfWithdraws;

      private CurrentWorkload(int transferWeight, int depositWeight, int withdrawWeight, int[] numberOfTransfers,
                              int[] numberOfDeposits, int[] numberOfWithdraws) {
         this.transferWeight = transferWeight;
         this.depositWeight = depositWeight;
         this.withdrawWeight = withdrawWeight;
         this.numberOfTransfers = numberOfTransfers;
         this.numberOfDeposits = numberOfDeposits;
         this.numberOfWithdraws = numberOfWithdraws;
      }

      public final int selectTransfer(Random random) {
         return operations(numberOfTransfers, random);
      }

      public final int selectDeposits(Random random) {
         return operations(numberOfDeposits, random);
      }

      public final int selectWithdraws(Random random) {
         return operations(numberOfWithdraws, random);
      }

      private int operations(int[] lowerAndUpperBound, Random random) {
         if (lowerAndUpperBound.length == 1) {
            return lowerAndUpperBound[0];
         }
         return random.nextInt(lowerAndUpperBound[1] - lowerAndUpperBound[0] + 1) + lowerAndUpperBound[0];
      }
   }
}
