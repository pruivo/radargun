package org.radargun.stages;

import org.radargun.stressors.AbstractPopulationStressor;
import org.radargun.stressors.BankPopulationStressor;

/**
 * //TODO document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class BankPopulationStage extends AbstractPopulationStage {

    private int initialAmount = 1000;

   public void setInitialAmount(int initialAmount) {
      this.initialAmount = initialAmount;
   }

   @Override
   public String toString() {
      return "BankPopulationStage{" +
            "initialAmount=" + initialAmount +
            ", " + super.toString();
   }

   @Override
   protected AbstractPopulationStressor createPopulationStressor() {
      BankPopulationStressor stressor = new BankPopulationStressor();
      stressor.setInitialAmount(initialAmount);
      return stressor;
   }
}
