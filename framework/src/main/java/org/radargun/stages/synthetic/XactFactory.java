package org.radargun.stages.synthetic;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public abstract class XactFactory <T extends XactParam, R extends Xact> {


   protected final T params;

   protected XactFactory(T params) {
      this.params = params;
   }

   public abstract R  buildXact(R last);
}
