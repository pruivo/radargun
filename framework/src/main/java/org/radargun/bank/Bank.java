package org.radargun.bank;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * @author pedro
 *         Date: 04-08-2011
 */
public class Bank {

    private static final String ACCOUNT_PREFIX = "ACCOUNT-";
    private static final String WORLD = "WORLD"; //this represents the money in the people wallet

    private int numberOfAccounts = 10;
    private final Random random;
    private int initialAmount = 1000;

    private static final Bank bank = new Bank();

    public static Bank getInstance() {
        return bank;
    }

    private Bank() {
        random = new Random(System.currentTimeMillis());
    }

    private String getAccountID(int id) {
        return ACCOUNT_PREFIX + id;
    }

    public void setNumberOfAccounts(int noa) {
        numberOfAccounts = noa;
    }

    public void setInitialAmmount(int ammount) {
        initialAmount = ammount;
    }

    public String getWorld() {
        return WORLD;
    }

    public String getOneAccount() {
        int acc_id = random.nextInt(numberOfAccounts);
        return getAccountID(acc_id);
    }

    public String[] getTwoAccount() {
        String[] accounts = new String[2];
        int acc_id = random.nextInt(numberOfAccounts);
        accounts[0] =  getAccountID(acc_id);
        acc_id = random.nextInt(numberOfAccounts);
        accounts[1] =  getAccountID(acc_id);
        return accounts;
    }

    public Set<String> getAccountsToWarmup(int slaveIdx, int nrActSlave) {
        Set<String> acc = new HashSet<String>();
        for(int i = slaveIdx; i < numberOfAccounts; i += nrActSlave) {
            acc.add(getAccountID(i));
        }
        return acc;
    }

    public Set<String> getAllAccounts() {
        Set<String> allAcc = new HashSet<String>();
        for(int i = 0; i < numberOfAccounts; ++i) {
            allAcc.add(getAccountID(i));
        }
        return allAcc;
    }

    public int getExpectedAmount() {
        return numberOfAccounts * initialAmount + initialAmount;
    }

    public int getInitialAmount() {
        return initialAmount;
    }
}
