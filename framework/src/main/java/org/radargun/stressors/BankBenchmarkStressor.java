package org.radargun.stressors;

import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.workloads.BankKeyGeneratorFactory;
import org.radargun.workloads.BankTransactionWorkload;
import org.radargun.workloads.BankTransactionWorkloadFactory;
import org.radargun.workloads.KeyGenerator;
import org.radargun.workloads.KeyGeneratorFactory;
import org.radargun.workloads.PopulationEntry;

import java.util.Iterator;
import java.util.Random;

/**
 * Stressor to benchmark a cache with a workload that simulates a bank
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class BankBenchmarkStressor extends AbstractBenchmarkStressor<BankTransactionWorkload, BankKeyGeneratorFactory,
      BankTransactionWorkloadFactory> {

   @ManagedOperation
   public final void setTransferWeight(int transferWeight) {
      transactionWorkload.setTransferWeight(transferWeight);
   }

   @ManagedOperation
   public final void setDepositWeight(int depositWeight) {
      transactionWorkload.setDepositWeight(depositWeight);
   }

   @ManagedOperation
   public final void setWithdrawWeight(int withdrawWeight) {
      transactionWorkload.setWithdrawWeight(withdrawWeight);
   }

   @ManagedOperation
   public final void setNumberOfTransfers(String transfers) {
      transactionWorkload.setNumberOfTransfers(transfers);
   }

   @ManagedOperation
   public final void setNumberOfDeposits(String deposits) {
      transactionWorkload.setNumberOfDeposits(deposits);
   }

   @ManagedOperation
   public final void setNumberOfWithdraws(String withdraws) {
      transactionWorkload.setNumberOfWithdraws(withdraws);
   }

   @Override
   protected final BankKeyGeneratorFactory createDefaultKeyGeneratorFactory() {
      return new BankKeyGeneratorFactory();
   }

   @Override
   protected final BankKeyGeneratorFactory cast(KeyGeneratorFactory keyGeneratorFactory) {
      return BankKeyGeneratorFactory.class.cast(keyGeneratorFactory);
   }

   @Override
   protected final BankTransactionWorkloadFactory createTransactionWorkloadFactory() {
      return new BankTransactionWorkloadFactory();
   }

   @Override
   protected final void executeTransaction(BankTransactionWorkload txWorkload, KeyGenerator keyGenerator) {
      switch (txWorkload.getType()) {
         case WITHDRAW:
            executeWithdraw(txWorkload, keyGenerator);
            break;
         case DEPOSIT:
            executeDeposit(txWorkload, keyGenerator);
            break;
         case TRANSFER:
            executeTransfer(txWorkload, keyGenerator);
            break;
         case CHECK_ALL_ACCOUNTS:
            executeCheckAccounts(txWorkload, keyGenerator);
            break;
         default:
            //TODO fuck you
      }
   }

   @Override
   protected final BankTransactionWorkload chooseNextTransaction(Random random) {
      return transactionWorkload.chooseTransaction(random, cacheWrapper);
   }

   private void executeWithdraw(BankTransactionWorkload txWorkload, KeyGenerator keyGenerator) {
      int numberOfOperations = txWorkload.getNumberOfOperations();

      begin(txWorkload);
      try {
         while (numberOfOperations-- > 0) {
            //account to world
            transfer(keyGenerator.getRandomKey(), keyGeneratorFactory.getWorld(), txWorkload.getThreadRandomGenerator(),
                     keyGenerator);
         }
      } catch (Throwable throwable) {
         txWorkload.markExecutionFailed();
      } finally {
         txWorkload.setEndExecutionTimestamp();
      }
      commit(txWorkload);

   }

   private void executeDeposit(BankTransactionWorkload txWorkload, KeyGenerator keyGenerator) {
      int numberOfOperations = txWorkload.getNumberOfOperations();

      begin(txWorkload);
      try {
         while (numberOfOperations-- > 0) {
            //world to account
            transfer(keyGeneratorFactory.getWorld(), keyGenerator.getRandomKey(), txWorkload.getThreadRandomGenerator(),
                     keyGenerator);
         }
      } catch (Throwable throwable) {
         txWorkload.markExecutionFailed();
      } finally {
         txWorkload.setEndExecutionTimestamp();
      }
      commit(txWorkload);

   }

   private void executeTransfer(BankTransactionWorkload txWorkload, KeyGenerator keyGenerator) {
      int numberOfOperations = txWorkload.getNumberOfOperations();

      begin(txWorkload);
      try {
         while (numberOfOperations-- > 0) {
            //account to account
            transfer(keyGenerator.getRandomKey(), keyGenerator.getRandomKey(), txWorkload.getThreadRandomGenerator(),
                     keyGenerator);
         }
      } catch (Throwable throwable) {
         txWorkload.markExecutionFailed();
      } finally {
         txWorkload.setEndExecutionTimestamp();
      }
      commit(txWorkload);

   }

   private int transfer(Object fromAccount, Object toAccount, Random random, KeyGenerator keyGenerator) throws Exception {
      //get the from amount
      int fromAmount = convertToInteger(cacheWrapper.get(keyGenerator.getBucket(), fromAccount));

      //calculate a random amount to transfer
      int amountToTransfer = random.nextInt(fromAmount);
      fromAmount -= amountToTransfer;

      //update from account
      cacheWrapper.put(keyGenerator.getBucket(), fromAccount, fromAmount);

      //get te to amount
      int toAmount = convertToInteger(cacheWrapper.get(keyGenerator.getBucket(), toAccount));
      toAmount += amountToTransfer;

      //update to account
      cacheWrapper.put(keyGenerator.getBucket(), toAccount, toAmount);
      return amountToTransfer;
   }

   private void executeCheckAccounts(BankTransactionWorkload txWorkload, KeyGenerator keyGenerator) {
      Iterator<PopulationEntry> iterator = keyGeneratorFactory.populateAll();
      int sum = 0;
      int expected = 0;

      begin(txWorkload);
      try {
         while (iterator.hasNext()) {
            Object account = iterator.next().getKey();
            sum += convertToInteger(cacheWrapper.get(keyGenerator.getBucket(), account));
            expected += keyGeneratorFactory.getInitialAmount();
         }

         sum += convertToInteger(cacheWrapper.get(keyGenerator.getBucket(), keyGeneratorFactory.getWorld()));
         expected += keyGeneratorFactory.getInitialAmount();
      } catch (Throwable throwable) {
         txWorkload.markExecutionFailed();
      } finally {
         txWorkload.setEndExecutionTimestamp();
      }
      commit(txWorkload);
      if (txWorkload.isExecutionOK() && txWorkload.isCommitOK() && sum != expected) {
         log.fatal("Inconsistent Snapshot Read");
      }
   }

   private int convertToInteger(Object amount) {
      if (amount == null) {
         return keyGeneratorFactory.getInitialAmount();
      }
      return Integer.class.cast(amount);
   }
}
