package org.radargun.tpcc;

import org.radargun.tpcc.transaction.NewOrderTransaction;
import org.radargun.tpcc.transaction.OrderStatusTransaction;
import org.radargun.tpcc.transaction.PaymentTransaction;
import org.radargun.tpcc.transaction.TpccTransaction;


/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 */
public class TpccTerminal {

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

   public TpccTransaction choiceTransaction() {

      double transactionType = TpccTools.doubleRandomNumber(0, 100);


      if (transactionType <= this.paymentWeight) {
         return new PaymentTransaction(this.indexNode, localWarehouseID);
      } else if (transactionType <= this.paymentWeight + this.orderStatusWeight) {
         return new OrderStatusTransaction(localWarehouseID);

      } else {
         return new NewOrderTransaction(localWarehouseID);
      }


   }
}