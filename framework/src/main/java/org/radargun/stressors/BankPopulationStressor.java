package org.radargun.stressors;

import org.radargun.workloads.BankKeyGeneratorFactory;

/**
 * The Population Stressor for the Bank Benchmark
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class BankPopulationStressor extends AbstractPopulationStressor<BankKeyGeneratorFactory> {

   public final void setInitialAmount(int initialAmount) {
      getFactory().setInitialAmount(initialAmount);
   }

   @Override
   protected final BankKeyGeneratorFactory createFactory() {
      return new BankKeyGeneratorFactory();
   }
}
