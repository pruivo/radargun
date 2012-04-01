package org.radargun.tpcc;

import org.radargun.CacheWrapper;

/**
 * Represents a tpcc domain object
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public interface DomainObject {

   /**
    * it stores the domain object in the cache wrapper
    * @param wrapper the cache wrapper
    * @throws Throwable if something wrong occurs
    */
   void store(CacheWrapper wrapper) throws Throwable;

   /**
    * it loads the domain object from the cache wrapper
    * @param wrapper the cache wrapper
    * @return true if the domain object was found, false otherwise
    * @throws Throwable if something wrong occurs
    */
   boolean load(CacheWrapper wrapper) throws Throwable;
}
