package org.radargun.bank.operation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.bank.Bank;

import java.io.InvalidClassException;

/**
 * @author pedro
 *         Date: 04-08-2011
 */
public abstract class AbstractBankOperation implements BankOperation{
    protected Bank bank;
    protected boolean success = false;
    protected Log log = LogFactory.getLog(getClass());

    public void init(Bank bank) {
        this.bank = bank;
    }

    public boolean isSuccessful() {
        return success;
    }

    protected int convertToLong(Object amount) throws InvalidClassException {
        if(amount == null) {
            return bank.getInitialAmount();
        }
        if(amount instanceof Integer) {
            return (Integer)amount;
        }
        throw new InvalidClassException("expected a integer but receives " + amount.getClass());
    }
}
