package org.radargun.bank.operation;

import org.radargun.CacheWrapper;

import java.util.Random;

/**
 * @author pedro
 *         Date: 04-08-2011
 */
public class DepositOperation extends AbstractBankOperation {

    public static final int ID = 2;

    public Object executeOn(CacheWrapper cache) throws Exception {
        Random r = new Random(System.currentTimeMillis());

        //withdraw from world
        String world = bank.getWorld();
        int worldAmmount = convertToLong(cache.get(null, world));
        int amountToDeposit = r.nextInt(worldAmmount);
        worldAmmount -= amountToDeposit;
        cache.put(null, world, worldAmmount);

        //deposit in account
        String account = bank.getOneAccount();
        int accountAmmount = convertToLong(cache.get(null, account));
        accountAmmount += amountToDeposit;
        cache.put(null, account, accountAmmount);

        success = true;
        return "deposit of " + amountToDeposit + " to account " + account;
    }

    public boolean isReadOnly() {
        return false;
    }

    public String getName() {
        return "Deposit Operation";
    }

    public int getType() {
        return ID;
    }
}
