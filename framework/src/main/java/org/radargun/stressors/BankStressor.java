package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.bank.BankOperationManager;
import org.radargun.bank.operation.BankOperation;

import java.util.List;
import java.util.Map;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 4.0
 */
public class BankStressor extends AbstractCacheWrapperStressor {

   private static final Log log = LogFactory.getLog(BankStressor.class);

   //percentage of each transaction. the remaining is for read only transaction   
   private int depositWeight = 0;
   private int withdrawWeight = 0;
   private int transferWeight = 50;

   @Override
   protected Stresser createStresser(int threadIdx) {
      return new BankStresser(threadIdx);
   }

   @Override
   protected void afterStresser(List<Stresser> stressers, CacheWrapper cacheWrapper) {

   }

   @Override
   protected void afterResults(List<Stresser> stressers, CacheWrapper cacheWrapper, Map<String, String> results) {

   }

   @Override
   protected void initBeforeStress() {

   }

   private class BankStresser extends Stresser {

      private final BankOperationManager manager;

      public BankStresser(int threadIndex) {
         super(threadIndex);
         manager = new BankOperationManager(transferWeight, depositWeight, withdrawWeight);
      }

      @Override
      protected TransactionResult executeTransaction(CacheWrapper cacheWrapper) {
         BankOperation op = manager.getNextOperation();
         boolean success, commitSuccess;
         long startCommit, endCommit;
         log.trace("Transaction type is " + op.getType());

         cacheWrapper.startTransaction();

         try {
            Object result = op.executeOn(cacheWrapper);
            log.trace("Transaction result is " + result);
            success = op.isSuccessful();
         } catch (Throwable t) {
            log.trace(t);
            success = false;
         }

         startCommit = System.nanoTime();

         try{
            cacheWrapper.endTransaction(success);
            commitSuccess = true;
         } catch(Throwable rb){
            commitSuccess = false;
            log.trace(rb);
         } finally {
            endCommit = System.nanoTime();
         }

         return new TransactionResult(success, commitSuccess, op.isReadOnly(), endCommit - startCommit);
      }
   }

   @Override
   public String toString() {
      return "BankStressor{" +
            "depositWeight=" + depositWeight +
            ", withdrawWeight=" + withdrawWeight +
            super.toString();
   }

   public void setDepositWeight(int depositWeight) {
      this.depositWeight = depositWeight;
   }

   public void setWithdrawWeight(int withdrawWeight) {
      this.withdrawWeight = withdrawWeight;
   }

   public void setTransferWeight(int transferWeight) {
      this.transferWeight = transferWeight;
   }
}
