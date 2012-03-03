package org.radargun.tpcc;

import org.radargun.CacheWrapper;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public interface DomainObject {
    void store(CacheWrapper wrapper) throws Throwable;
    boolean load(CacheWrapper wrapper) throws Throwable;
}
