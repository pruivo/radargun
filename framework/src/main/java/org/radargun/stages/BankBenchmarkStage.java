package org.radargun.stages;

import org.radargun.stressors.AbstractBenchmarkStressor;
import org.radargun.stressors.BankBenchmarkStressor;
import org.radargun.workloads.KeyGeneratorFactory;

/**
 * Stage that performs a web session with a bank workload
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class BankBenchmarkStage extends AbstractBenchmarkStage {

   //the percentage of transfer transactions
   private int transferPercentage = 50;
   //the percentage of deposit transactions
   private int depositPercentage = 0;
   //the percentage of withdraw transactions
   private int withdrawPercentage = 0;
   //the lower and upper bound of the number of transfer per transaction
   private String numberOfTransfers = "1,1";
   //the lower and upper bound of the number of deposit per transaction
   private String numberOfDeposits = "1,1";
   //the lower and upper bound of the number of withdraw per transaction
   private String numberOfWithdraws = "1,1";

   @Override
   public final String toString() {
      return "BankBenchmarkStage{" +
            "transferPercentage=" + transferPercentage +
            ", depositPercentage=" + depositPercentage +
            ", withdrawPercentage=" + withdrawPercentage +
            ", numberOfTransfers='" + numberOfTransfers + '\'' +
            ", numberOfDeposits='" + numberOfDeposits + '\'' +
            ", numberOfWithdraws='" + numberOfWithdraws + '\'' +
            super.toString();
   }

   public final void setTransferPercentage(int transferPercentage) {
      this.transferPercentage = transferPercentage;
   }

   public final void setDepositPercentage(int depositPercentage) {
      this.depositPercentage = depositPercentage;
   }

   public final void setWithdrawPercentage(int withdrawPercentage) {
      this.withdrawPercentage = withdrawPercentage;
   }

   public final void setNumberOfTransfers(String numberOfTransfers) {
      this.numberOfTransfers = numberOfTransfers;
   }

   public final void setNumberOfDeposits(String numberOfDeposits) {
      this.numberOfDeposits = numberOfDeposits;
   }

   public void setNumberOfWithdraws(String numberOfWithdraws) {
      this.numberOfWithdraws = numberOfWithdraws;
   }

   @Override
   protected final AbstractBenchmarkStressor createStressor(KeyGeneratorFactory keyGeneratorFactory) {
      BankBenchmarkStressor bankStressor = new BankBenchmarkStressor();
      bankStressor.setDepositWeight(depositPercentage);
      bankStressor.setTransferWeight(transferPercentage);
      bankStressor.setWithdrawWeight(withdrawPercentage);
      bankStressor.setNumberOfTransfers(numberOfTransfers);
      bankStressor.setNumberOfDeposits(numberOfDeposits);
      bankStressor.setNumberOfWithdraws(numberOfWithdraws);
      return bankStressor;
   }
}
