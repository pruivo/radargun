package org.radargun.bank.operation;

import org.radargun.CacheWrapper;

/**
 * @author pedro
 *         Date: 04-08-2011
 */
public interface BankOperation {
    Object executeOn(CacheWrapper cache) throws Exception;
    boolean isSuccessful();
    boolean isReadOnly();
    String getName();
    int getType();
}
