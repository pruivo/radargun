package org.radargun.stages.synthetic;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public abstract class XactFactory <T extends XactParam, R extends Xact> {

   public abstract R  buildXact(T param, R last);
}
