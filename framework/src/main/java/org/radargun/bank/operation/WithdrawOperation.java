package org.radargun.bank.operation;

import org.radargun.CacheWrapper;

import java.util.Random;

/**
 * @author pedro
 *         Date: 04-08-2011
 */
public class WithdrawOperation extends AbstractBankOperation {

    public static final int ID = 4;

    public Object executeOn(CacheWrapper cache) throws Exception {
        Random r = new Random(System.currentTimeMillis());

        //withdraw from account
        String account = bank.getOneAccount();
        int accountAmmount = convertToLong(cache.get(null, account));
        int amountToWithdraw = r.nextInt((int) accountAmmount);
        accountAmmount -= amountToWithdraw;
        cache.put(null, account, accountAmmount);

        //deposit in world
        String world = bank.getWorld();
        int worldAmmount = convertToLong(cache.get(null, world));
        worldAmmount += amountToWithdraw;
        cache.put(null, world, worldAmmount);

        success = true;
        return "withdraw of " + amountToWithdraw + " from account " + account;
    }

    public boolean isReadOnly() {
        return false;
    }

    public String getName() {
        return "Withdraw Operation";
    }

    public int getType() {
        return ID;
    }
}
