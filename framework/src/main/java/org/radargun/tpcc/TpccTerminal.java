package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.tpcc.transaction.NewOrderTransaction;
import org.radargun.tpcc.transaction.OrderStatusTransaction;
import org.radargun.tpcc.transaction.PaymentTransaction;
import org.radargun.tpcc.transaction.TpccTransaction;


/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 */
public class TpccTerminal {

   private static Log log = LogFactory.getLog(TpccTerminal.class);

   public final static int NEW_ORDER = 1, PAYMENT = 2, ORDER_STATUS = 3, DELIVERY = 4, STOCK_LEVEL = 5;

   public final static String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

   private double paymentWeight;

   private double orderStatusWeight;

   private int indexNode;

   private int localWarehouseID;


   public TpccTerminal(double paymentWeight, double orderStatusWeight, int indexNode, int localWarehouseID) {

      this.paymentWeight = paymentWeight;
      this.orderStatusWeight = orderStatusWeight;
      this.indexNode = indexNode;
      this.localWarehouseID = localWarehouseID;
   }

   public TpccTransaction choiceTransaction(boolean canExecuteReadOnlyTx, boolean canExecuteWriteTx) {

      if (!canExecuteWriteTx && !canExecuteReadOnlyTx) {
         throw new IllegalArgumentException("This cache wrapper must be able to execute read-only or write transaction");
      }

      double transactionType = Math.min(TpccTools.doubleRandomNumber(1, 100), 100.0);

      double realPaymentWeight = paymentWeight, realOrderStatusWeight = orderStatusWeight;

      if (canExecuteWriteTx && !canExecuteReadOnlyTx) {
         realPaymentWeight = paymentWeight + (orderStatusWeight / 2);
         realOrderStatusWeight = 0;
      } else if (!canExecuteWriteTx && canExecuteReadOnlyTx) {
         realPaymentWeight = 0;
         realOrderStatusWeight = 100;
      }

      log.debug("Choose transaction " + transactionType +
                      ". Payment Weight=" + realPaymentWeight + "(" + paymentWeight + ")" +
                      ", Order Status Weight=" + realOrderStatusWeight + "(" + orderStatusWeight + ")" +
                      ", can execute: read only tx? " + canExecuteReadOnlyTx + ", write tx?" + canExecuteWriteTx);

      if (transactionType <= realPaymentWeight) {
         return new PaymentTransaction(this.indexNode, localWarehouseID);
      } else if (transactionType <= realPaymentWeight + realOrderStatusWeight) {
         return new OrderStatusTransaction(localWarehouseID);
      } else {
         return new NewOrderTransaction(localWarehouseID);
      }
   }

}