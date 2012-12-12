package org.radargun.workloads;

import java.util.Random;

import static org.radargun.workloads.BankTransactionWorkloadFactory.TransactionType;

/**
 * Represents a transaction which simulates a bank workload. In this case, the transaction can be one of the following
 * types:
 *  - transfer between two accounts
 *  - deposit in one account
 *  - withdraw of one account
 *  - check all accounts (no money lost during execution)
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class BankTransactionWorkload extends AbstractTransactionWorkload {

   private final int numberOfOperations;
   private final TransactionType type;

   public BankTransactionWorkload(boolean readOnly, Random threadRandomGenerator, int numberOfOperations,
                                  TransactionType type) {
      super(readOnly, threadRandomGenerator);
      this.numberOfOperations = numberOfOperations;
      this.type = type;
   }

   public final int getNumberOfOperations() {
      return numberOfOperations;
   }

   public final TransactionType getType() {
      return type;
   }
}
