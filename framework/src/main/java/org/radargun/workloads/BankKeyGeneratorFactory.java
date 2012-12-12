package org.radargun.workloads;

import java.util.Random;

/**
 * A Key Generator Factory for the bank benchmark
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class BankKeyGeneratorFactory extends KeyGeneratorFactory {

   private int initialAmount;

   @Override
   public final String toString() {
      return "BankKeyGeneratorFactory{" +
            "initialAmount=" + initialAmount +
            ", " + super.toString() + "}";
   }

   public final Object getWorld() {
      return createKey(-1, -1, -1);
   }

   public final int getInitialAmount() {
      return initialAmount;
   }

   public final void setInitialAmount(int initialAmount) {
      this.initialAmount = initialAmount;
   }

   @Override
   protected final Object getRandomValue(Random random) {
      return initialAmount;
   }
}
