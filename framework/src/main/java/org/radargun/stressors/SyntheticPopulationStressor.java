package org.radargun.stressors;

import org.radargun.workloads.KeyGeneratorFactory;

/**
 * The warmup that initialize the keys used by the synthetic benchmark
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class SyntheticPopulationStressor extends AbstractPopulationStressor<KeyGeneratorFactory> {

   @Override
   public final String toString() {
      return "SyntheticPopulationStressor{" + super.toString();
   }

   @Override
   protected final KeyGeneratorFactory createFactory() {
      return new KeyGeneratorFactory();
   }
}
