package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.bank.Bank;
import org.radargun.stages.WarmupStage;

import java.util.Collections;
import java.util.Map;

/**
 * @author Pedro
 */
public class BankWarmupStressor implements CacheWrapperStressor {

    private static Log log = LogFactory.getLog(WarmupStage.class);

    private int numberOfAccounts = 10;
    private int initialAmmount = 1000;

    private int slaveIndex;
    private int nrActiveSlaves;

    public BankWarmupStressor() { //for local configuration
        this.slaveIndex = 0;
        this.nrActiveSlaves = 1;
    }

    public BankWarmupStressor(int slaveIndex, int nrActiveSlaves) {
        this.slaveIndex = slaveIndex;
        this.nrActiveSlaves = nrActiveSlaves;
    }

    public Map<String, String> stress(CacheWrapper wrapper) {
        Bank bank = Bank.getInstance();
        bank.setInitialAmmount(initialAmmount);
        bank.setNumberOfAccounts(numberOfAccounts);

        for(String account : bank.getAccountsToWarmup(slaveIndex, nrActiveSlaves)) {
            try {
                wrapper.put(null, account, initialAmmount);
            } catch (Exception e) {
                log.warn("account " + account + " does not initate successfully");
                log.warn(e);
            }
        }

        if(slaveIndex == 0) {
            try {
                wrapper.put(null, bank.getWorld(), initialAmmount);
            } catch (Exception e) {
                log.warn("world does not initate successfully");
                log.warn(e);
            }
        }
        return Collections.emptyMap();
    }

    public void setNumberOfAccounts(int numberOfAccounts) {
        this.numberOfAccounts = numberOfAccounts;
    }

    public void setInitialAmmount(int initialAmmount) {
        this.initialAmmount = initialAmmount;
    }

    @Override
    public String toString() {
        return "BankWarmupStressor{" +
                "numberOfAccounts=" + numberOfAccounts +
                "initialAmmount" + initialAmmount + "}";
    }


    public void destroy() throws Exception {}
}
