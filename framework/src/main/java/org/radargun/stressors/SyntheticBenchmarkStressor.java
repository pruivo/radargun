package org.radargun.stressors;

import org.radargun.jmx.annotations.MBean;
import org.radargun.jmx.annotations.ManagedOperation;
import org.radargun.workloads.KeyGenerator;
import org.radargun.workloads.KeyGeneratorFactory;
import org.radargun.workloads.SyntheticTransactionWorkload;
import org.radargun.workloads.SyntheticTransactionWorkloadFactory;

import java.util.Random;

/**
 * On multiple threads executes put and get operations against the CacheWrapper, and returns the result as an Map.
 *
 * @author Pedro Ruivo
 * @since 1.0
 */
@MBean(objectName = "Benchmark", description = "Executes a defined workload over a cache")
public class SyntheticBenchmarkStressor extends AbstractBenchmarkStressor<SyntheticTransactionWorkload,
      KeyGeneratorFactory, SyntheticTransactionWorkloadFactory> {

   public SyntheticBenchmarkStressor(KeyGeneratorFactory keyGeneratorFactory) {
      super(keyGeneratorFactory);
   }

   @ManagedOperation
   public final void setWriteTransactionReads(String reads) {
      transactionWorkload.setWriteTransactionReads(reads);
   }

   @ManagedOperation
   public final void setWriteTransactionWrites(String writes) {
      transactionWorkload.setWriteTransactionwrites(writes);
   }

   @ManagedOperation
   public final void setReadTransactionsReads(String reads) {
      transactionWorkload.setReadTransactionsReads(reads);
   }

   @ManagedOperation
   public final void setWriteTransactionPercentage(int writeTransactionPercentage) {
      transactionWorkload.setWriteTransactionPercentage(writeTransactionPercentage);
   }

   @Override
   protected final KeyGeneratorFactory createDefaultKeyGeneratorFactory() {
      return new KeyGeneratorFactory();
   }

   @Override
   protected final KeyGeneratorFactory cast(KeyGeneratorFactory keyGeneratorFactory) {
      return keyGeneratorFactory;
   }

   @Override
   protected final SyntheticTransactionWorkloadFactory createTransactionWorkloadFactory() {
      return new SyntheticTransactionWorkloadFactory();
   }

   @Override
   protected final void executeTransaction(SyntheticTransactionWorkload txWorkload, KeyGenerator keyGenerator) {
      begin(txWorkload);
      try {
         while(txWorkload.hasNext()){
            Object key = keyGenerator.getRandomKey();

            if (txWorkload.isNextOperationARead()) {
               cacheWrapper.get(keyGenerator.getBucket(), key);
            } else {
               Object payload = keyGenerator.getRandomValue();
               cacheWrapper.put(keyGenerator.getBucket(), key, payload);

            }
         }
      } catch (Exception e) {
         txWorkload.markExecutionFailed();
      } finally {
         txWorkload.setEndExecutionTimestamp();
      }
      commit(txWorkload);
   }

   @Override
   protected final SyntheticTransactionWorkload chooseNextTransaction(Random random) {
      return transactionWorkload.chooseTransaction(random, cacheWrapper);
   }
}

