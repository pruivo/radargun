package org.radargun.stages;

import org.radargun.stressors.AbstractCacheWrapperStressor;
import org.radargun.stressors.BankStressor;

/** 
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class BankBenchmarkStage extends AbstractBenchmarkStage {

   private int transferPercentage = 30;
   private int depositPercentage = 15;
   private int withdrawPercentage = 15;

   @Override
   protected AbstractCacheWrapperStressor createStressor() {
      BankStressor bankStressor = new BankStressor();
      bankStressor.setDepositWeight(depositPercentage);
      bankStressor.setTransferWeight(transferPercentage);
      bankStressor.setWithdrawWeight(withdrawPercentage);
      return bankStressor;
   }

   @Override
   public String toString() {
      return "BankBenchmarkStage{" +
            "transferPercentage=" + transferPercentage +
            ", depositPercentage=" + depositPercentage +
            ", withdrawPercentage=" + withdrawPercentage +
            super.toString();
   }

   public void setTransferPercentage(int transferPercentage) {
      this.transferPercentage = transferPercentage;
   }

   public void setDepositPercentage(int depositPercentage) {
      this.depositPercentage = depositPercentage;
   }

   public void setWithdrawPercentage(int withdrawPercentage) {
      this.withdrawPercentage = withdrawPercentage;
   }


}
