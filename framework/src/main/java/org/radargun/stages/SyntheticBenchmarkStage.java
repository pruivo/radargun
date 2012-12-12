package org.radargun.stages;

import org.radargun.stressors.AbstractBenchmarkStressor;
import org.radargun.stressors.SyntheticBenchmarkStressor;
import org.radargun.workloads.KeyGeneratorFactory;

/**
 * Simulates the work with a distributed web sessions.
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class SyntheticBenchmarkStage extends AbstractBenchmarkStage {

   //the lower and upper bound of write operation in read-write transaction
   private String writeTxWrites = "50,50";
   //the lower and upper bound of read operation in read-write transaction
   private String writeTxReads = "50,50";
   //the lower and upper bound of read operation in read-only transaction
   private String readTxReads = "100,100";
   //The percentage of write transactions generated
   private int writeTransactionPercentage = 100;

   public final void setWriteTxWrites(String writeTxWrites) {
      this.writeTxWrites = writeTxWrites;
   }

   public final void setWriteTxReads(String writeTxReads) {
      this.writeTxReads = writeTxReads;
   }

   public final void setReadTxReads(String readTxReads) {
      this.readTxReads = readTxReads;
   }

   public final void setWriteTransactionPercentage(int writeTransactionPercentage) {
      this.writeTransactionPercentage = writeTransactionPercentage;
   }

   @Override
   public final String toString() {
      return "SyntheticBenchmarkStage{" +
            "writeTxWrites='" + writeTxWrites + '\'' +
            ", writeTxReads='" + writeTxReads + '\'' +
            ", readTxReads='" + readTxReads + '\'' +
            ", writeTransactionPercentage=" + writeTransactionPercentage +
            super.toString();
   }

   @Override
   protected final AbstractBenchmarkStressor createStressor(KeyGeneratorFactory keyGeneratorFactory) {
      SyntheticBenchmarkStressor stressor = new SyntheticBenchmarkStressor(keyGeneratorFactory);
      stressor.setWriteTransactionwrites(writeTxWrites);
      stressor.setWriteTransactionReads(writeTxReads);
      stressor.setReadTransactionsReads(readTxReads);
      stressor.setWriteTransactionPercentage(writeTransactionPercentage);
      return stressor;
   }
}
