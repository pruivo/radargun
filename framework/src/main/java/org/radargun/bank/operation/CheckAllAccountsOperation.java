package org.radargun.bank.operation;

import org.radargun.CacheWrapper;

/**
 * @author pedro
 *         Date: 04-08-2011
 */
public class CheckAllAccountsOperation extends AbstractBankOperation {

    public static final int ID = 1;

    public Object executeOn(CacheWrapper cache) throws Exception {
        int all = 0;
        for(String account : bank.getAllAccounts()) {
            all += convertToLong(cache.get(null, account));
        }

        all += convertToLong(cache.get(null, bank.getWorld()));

        if(all != bank.getExpectedAmount()) {
            log.warn("check all accounts fails. amount calculated=" + all + ", amount expected=" + bank.getExpectedAmount());
        }

        success = true;
        return all;
    }

    public boolean isReadOnly() {
        return true;
    }

    public String getName() {
        return "Check all accounts";
    }

    public int getType() {
        return ID;
    }
}
