package org.radargun.stages.synthetic;

import java.util.Random;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticXactParams extends XactParam{

   private boolean allowBlindWrites;
   private Random random;
   private XACT_RETRY xact_retry;

   public boolean isAllowBlindWrites() {
      return allowBlindWrites;
   }

   public void setAllowBlindWrites(boolean allowBlindWrites) {
      this.allowBlindWrites = allowBlindWrites;
   }

   public Random getRandom() {
      return random;
   }

   public void setRandom(Random random) {
      this.random = random;
   }

   public XACT_RETRY getXact_retry() {
      return xact_retry;
   }

   public void setXact_retry(XACT_RETRY xact_retry) {
      this.xact_retry = xact_retry;
   }
}
