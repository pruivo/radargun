package org.radargun.bank;

import org.radargun.bank.operation.*;

import java.util.Random;

/**
 * @author pedro
 *         Date: 04-08-2011
 */
public class BankOperationManager {

    private int transferWeight;
    private int depositWeight;
    private int withdrawWeight;
    private final Random random;

    public BankOperationManager(int transferWeight, int depositWeight, int withdrawWeight) {
        this.transferWeight = transferWeight;
        this.depositWeight = depositWeight;
        this.withdrawWeight = withdrawWeight;
        this.random = new Random(System.currentTimeMillis());
    }

    public BankOperation getNextOperation() {
        int nextOp = random.nextInt(100);
        if(nextOp < transferWeight) {
            TransferOperation top = new TransferOperation();
            top.init(Bank.getInstance());
            return top;
        } else if (nextOp < (transferWeight + depositWeight)) {
            DepositOperation dop = new DepositOperation();
            dop.init(Bank.getInstance());
            return dop;
        } else if (nextOp < (transferWeight + depositWeight + withdrawWeight)) {
            WithdrawOperation wop = new WithdrawOperation();
            wop.init(Bank.getInstance());
            return wop;
        } else {
            CheckAllAccountsOperation cop = new CheckAllAccountsOperation();
            cop.init(Bank.getInstance());
            return cop;
        }
    }

    public int getTransferWeight() {
        return transferWeight;
    }

    public int getDepositWeight() {
        return depositWeight;
    }

    public int getWithdrawWeight() {
        return withdrawWeight;
    }
}
