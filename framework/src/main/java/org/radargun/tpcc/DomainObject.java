package org.radargun.tpcc;

import org.radargun.CacheWrapper;
import org.radargun.tpcc.domain.TpccKey;

import java.io.Serializable;

/**
 * Represents a tpcc domain object
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public abstract class DomainObject<T extends DomainObject> implements Serializable {

   private transient TpccKey tpccKey;   

   /**
    * it stores the domain object in the cache wrapper
    * @param wrapper the cache wrapper
    * @throws Throwable if something wrong occurs
    */
   public void store(CacheWrapper wrapper) throws Throwable {
      internalStore(wrapper);
   }

   /**
    * it stores the domain object in the cache wrapper
    *
    * @param wrapper    the cache wrapper
    * @param nodeIndex  the node index which stores this object               
    * @throws Throwable if something wrong occurs
    */
   public void store(CacheWrapper wrapper, int nodeIndex) throws Throwable {
      internalStore(wrapper);
   }

   /**
    * it loads the domain object from the cache wrapper
    * @param wrapper the cache wrapper
    * @return true if the domain object was found, false otherwise
    * @throws Throwable if something wrong occurs
    */
   public abstract boolean load(CacheWrapper wrapper) throws Throwable;

   private synchronized TpccKey getTpccKey() {
      if (tpccKey == null) {
         tpccKey = createTpccKey();
      }
      return tpccKey;
   }

   protected final void internalStore(CacheWrapper cacheWrapper) throws Exception {
      cacheWrapper.put(null, getTpccKey(), this);
   }

   @SuppressWarnings("unchecked")
   protected final T internalLoad(CacheWrapper cacheWrapper) throws Exception {
      return (T) cacheWrapper.get(null, getTpccKey());
   }

   protected abstract TpccKey createTpccKey();
}
