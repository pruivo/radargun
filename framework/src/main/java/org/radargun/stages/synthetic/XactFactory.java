package org.radargun.stages.synthetic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public abstract class XactFactory <T extends XactParam, R extends Xact> {


   protected final T params;
   protected final static Log log = LogFactory.getLog(XactFactory.class);

   protected XactFactory(T params) {
      this.params = params;
      log.trace("Factory built "+params);
   }

   public abstract R  buildXact(R last);
}
