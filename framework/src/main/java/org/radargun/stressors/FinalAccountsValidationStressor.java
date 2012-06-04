package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.bank.Bank;
import org.radargun.bank.operation.CheckAllAccountsOperation;

import java.util.HashMap;
import java.util.Map;

/**
 * @author pedro
 *         Date: 05-08-2011
 */
public class FinalAccountsValidationStressor implements CacheWrapperStressor {

    private static Log log = LogFactory.getLog(FinalAccountsValidationStressor.class);

    public Map<String, String> stress(CacheWrapper wrapper) {
        Bank bank = Bank.getInstance();
        Map<String, String> result = new HashMap<String, String>();

        CheckAllAccountsOperation op = new CheckAllAccountsOperation();
        op.init(bank);
        try {
            Integer allAmount = (Integer)op.executeOn(wrapper);
            Integer expected = bank.getExpectedAmount();

            if(!allAmount.equals(expected)) {
                log.error("the total amount of money differs for the expected: total=" + allAmount + ", expected=" + expected);
            } else {
                log.info(" +++ validation sucessfull!! +++ ");
            }

            result.put(expected.toString(), allAmount.toString());
        } catch (Exception e) {
            log.warn(e);
            result.clear();
        }
        return result;
    }

    public void destroy() throws Exception {}
}
