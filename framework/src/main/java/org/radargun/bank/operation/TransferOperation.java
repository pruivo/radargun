package org.radargun.bank.operation;

import org.radargun.CacheWrapper;

import java.util.Random;

/**
 * @author pedro
 *         Date: 04-08-2011
 */
public class TransferOperation extends AbstractBankOperation {

    public static final int ID = 3;

    public Object executeOn(CacheWrapper cache) throws Exception {
        Random r = new Random(System.currentTimeMillis());
        String[] accounts = bank.getTwoAccount();

        //withdraw from account
        int accountAmmount0 = convertToLong(cache.get(null, accounts[0]));
        int amountToTransfer = r.nextInt(accountAmmount0);
        accountAmmount0 -= amountToTransfer;
        cache.put(null, accounts[0], accountAmmount0);

        //deposit in account
        int accountAmmount1 = convertToLong(cache.get(null, accounts[1]));
        accountAmmount1 += amountToTransfer;
        cache.put(null, accounts[1], accountAmmount1);

        success = true;
        return "transfer " + amountToTransfer + " from " + accounts[0] + " to " + accounts[1];
    }

    public boolean isReadOnly() {
        return false;
    }

    public String getName() {
        return "Transfer Operation";
    }

    public int getType() {
        return ID;
    }
}