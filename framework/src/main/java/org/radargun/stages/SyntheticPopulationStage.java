package org.radargun.stages;

import org.radargun.stressors.AbstractPopulationStressor;
import org.radargun.stressors.SyntheticPopulationStressor;

/**
 * Performs a warmup in the cache, initializing all the keys used by benchmarking
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class SyntheticPopulationStage extends AbstractPopulationStage {

   @Override
   public String toString() {
      return "SyntheticBenchmarkStage {" + super.toString();
   }

   @Override
   protected AbstractPopulationStressor createPopulationStressor() {
      return new SyntheticPopulationStressor();
   }
}
