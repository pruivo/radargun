package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.tpcc.domain.*;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Date;

import static org.radargun.utils.Utils.memString;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Diego Didona, didona@gsd.inesc-id.pt
 * @author Pedro Ruivo
 */
public class TpccPopulation {

   private static Log log = LogFactory.getLog(TpccPopulation.class);

   private long POP_C_LAST = TpccTools.NULL_NUMBER;

   private long POP_C_ID = TpccTools.NULL_NUMBER;

   private long POP_OL_I_ID = TpccTools.NULL_NUMBER;

   protected boolean _new_order = false;

   private final int _seqIdCustomer[];

   protected final CacheWrapper wrapper;

   private final MemoryMXBean memoryBean;

   protected final int numWarehouses;

   protected final int slaveIndex;
   protected final int numSlaves;

   protected final long cLastMask;
   protected final long olIdMask;
   protected final long cIdMask;

   protected final TpccTools tpccTools;

   public TpccPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, int numSlaves, long cLastMask,
                         long olIdMask, long cIdMask) {
      this.wrapper = wrapper;
      this._seqIdCustomer = new int[TpccTools.NB_MAX_CUSTOMER];
      this.memoryBean = ManagementFactory.getMemoryMXBean();
      this.numWarehouses = numWarehouses;
      this.slaveIndex = slaveIndex;
      this.numSlaves = numSlaves;
      this.cLastMask = cLastMask;
      this.olIdMask = olIdMask;
      this.cIdMask = cIdMask;
      tpccTools = TpccTools.newInstance();
   }

   public final void initTpccTools() {
      TpccTools.NB_WAREHOUSES = this.numWarehouses;
      TpccTools.A_C_LAST = this.cLastMask;
      TpccTools.A_OL_I_ID = this.olIdMask;
      TpccTools.A_C_ID = this.cIdMask;
   }

   public void performPopulation(){
      initializeToolsParameters();

      populateItem();

      populateWarehouses();

      System.gc();
   }


   protected void initializeToolsParameters() {
      initTpccTools();

      if (this.slaveIndex == 0) {//Only one slave
         long c_c_last = tpccTools.randomNumber(0, TpccTools.A_C_LAST);
         long c_c_id = tpccTools.randomNumber(0, TpccTools.A_C_ID);
         long c_ol_i_id = tpccTools.randomNumber(0, TpccTools.A_OL_I_ID);

         boolean successful = false;
         while (!successful) {
            try {
               wrapper.put(null, "C_C_LAST", c_c_last);
               successful = true;
            } catch (Throwable e) {
               log.warn(e);
            }
         }

         successful = false;
         while (!successful) {
            try {
               wrapper.put(null, "C_C_ID", c_c_id);
               successful = true;
            } catch (Throwable e) {
               log.warn(e);
            }
         }

         successful = false;
         while (!successful) {
            try {
               wrapper.put(null, "C_OL_ID", c_ol_i_id);
               successful = true;
            } catch (Throwable e) {
               log.warn(e);
            }
         }

      }
   }

   public String c_last() {
      String c_last = "";
      long number = tpccTools.nonUniformRandom(getC_LAST(), TpccTools.A_C_LAST, TpccTools.MIN_C_LAST, TpccTools.MAX_C_LAST);
      String alea = String.valueOf(number);
      while (alea.length() < 3) {
         alea = "0" + alea;
      }
      for (int i = 0; i < 3; i++) {
         c_last += TpccTools.C_LAST[Integer.parseInt(alea.substring(i, i + 1))];
      }
      return c_last;
   }

   public long getC_LAST() {
      if (POP_C_LAST == TpccTools.NULL_NUMBER) {
         POP_C_LAST = tpccTools.randomNumber(TpccTools.MIN_C_LAST, TpccTools.A_C_LAST);
      }
      return POP_C_LAST;
   }

   public long getC_ID() {
      if (POP_C_ID == TpccTools.NULL_NUMBER) {
         POP_C_ID = tpccTools.randomNumber(0, TpccTools.A_C_ID);
      }
      return POP_C_ID;
   }

   public long getOL_I_ID() {
      if (POP_OL_I_ID == TpccTools.NULL_NUMBER) {
         POP_OL_I_ID = tpccTools.randomNumber(0, TpccTools.A_OL_I_ID);
      }
      return POP_OL_I_ID;
   }

   protected void populateItem() {
      log.trace("Populate Items");

      long init_id_item = 1;
      long num_of_items = TpccTools.NB_MAX_ITEM;

      if (numSlaves > 1) {
         num_of_items = TpccTools.NB_MAX_ITEM / numSlaves;
         long remainder = TpccTools.NB_MAX_ITEM % numSlaves;

         init_id_item = (slaveIndex * num_of_items) + 1;

         if (slaveIndex == numSlaves - 1) {
            num_of_items += remainder;
         }


      }
      logItemsPopulation(init_id_item, init_id_item - 1 + num_of_items);
      for (long i = init_id_item; i <= (init_id_item - 1 + num_of_items); i++) {

         Item newItem = new Item(i, tpccTools.aleaNumber(1, 10000), tpccTools.aleaChainec(14, 24),
                                 tpccTools.aleaFloat(1, 100, 2), tpccTools.sData());
         txAwarePut(newItem);
      }
      printMemoryInfo();
   }

   protected void populateWarehouses() {
      log.trace("Populate warehouses");
      if (this.numWarehouses > 0) {
         for (int i = 1; i <= this.numWarehouses; i++) {
            log.info("Populate Warehouse " + i);
            if (this.slaveIndex == 0) {// Warehouse assigned to node 0 if I have more than one node
               Warehouse newWarehouse = new Warehouse(i,
                                                      tpccTools.aleaChainec(6, 10),
                                                      tpccTools.aleaChainec(10, 20), tpccTools.aleaChainec(10, 20),
                                                      tpccTools.aleaChainec(10, 20), tpccTools.aleaChainel(2, 2),
                                                      tpccTools.aleaChainen(4, 4) + TpccTools.CHAINE_5_1,
                                                      tpccTools.aleaFloat(Float.valueOf("0.0000"), Float.valueOf("0.2000"), 4),
                                                      TpccTools.WAREHOUSE_YTD);
               txAwarePut(newWarehouse);
            }
            populateStock(i);

            populateDistricts(i);

            printMemoryInfo();
         }
      }
   }

   protected void populateStock(int id_warehouse) {
      if (id_warehouse < 0) {
         log.warn("Trying to populate Stock for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating Stock for warehouse " + id_warehouse);

      long init_id_item = 1;
      long num_of_items = TpccTools.NB_MAX_ITEM;

      if (numSlaves > 1) {
         num_of_items = TpccTools.NB_MAX_ITEM / numSlaves;
         long remainder = TpccTools.NB_MAX_ITEM % numSlaves;

         init_id_item = (slaveIndex * num_of_items) + 1;

         if (slaveIndex == numSlaves - 1) {
            num_of_items += remainder;
         }


      }
      logStockPopulation(id_warehouse, init_id_item, init_id_item - 1 + num_of_items);
      for (long i = init_id_item; i <= (init_id_item - 1 + num_of_items); i++) {

         Stock newStock = new Stock(i,
                                    id_warehouse,
                                    tpccTools.aleaNumber(10, 100),
                                    tpccTools.aleaChainel(24, 24),
                                    tpccTools.aleaChainel(24, 24),
                                    tpccTools.aleaChainel(24, 24),
                                    tpccTools.aleaChainel(24, 24),
                                    tpccTools.aleaChainel(24, 24),
                                    tpccTools.aleaChainel(24, 24),
                                    tpccTools.aleaChainel(24, 24),
                                    tpccTools.aleaChainel(24, 24),
                                    tpccTools.aleaChainel(24, 24),
                                    tpccTools.aleaChainel(24, 24),
                                    0,
                                    0,
                                    0,
                                    tpccTools.sData());
         txAwarePut(newStock);
      }
   }

   protected void populateDistricts(int id_warehouse) {
      if (id_warehouse < 0) {
         log.warn("Trying to populate Districts for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating District for warehouse " + id_warehouse);

      int init_id_district = 1;
      int num_of_districts = TpccTools.NB_MAX_DISTRICT;

      if (numSlaves > 1) {
         num_of_districts = TpccTools.NB_MAX_DISTRICT / numSlaves;
         int remainder = TpccTools.NB_MAX_DISTRICT % numSlaves;

         if (slaveIndex <= remainder) {
            init_id_district = (slaveIndex * (num_of_districts + 1)) + 1;
         } else {
            init_id_district = (((remainder) * (num_of_districts + 1)) + ((slaveIndex - remainder) * num_of_districts)) + 1;
         }
         if (slaveIndex < remainder) {
            num_of_districts += 1;
         }
      }
      logDistrictPopulation(id_warehouse, init_id_district, init_id_district - 1 + num_of_districts);
      for (int id_district = init_id_district; id_district <= (init_id_district - 1 + num_of_districts); id_district++) {
         District newDistrict = new District(id_warehouse,
                                             id_district,
                                             tpccTools.aleaChainec(6, 10),
                                             tpccTools.aleaChainec(10, 20),
                                             tpccTools.aleaChainec(10, 20),
                                             tpccTools.aleaChainec(10, 20),
                                             tpccTools.aleaChainel(2, 2),
                                             tpccTools.aleaChainen(4, 4) + TpccTools.CHAINE_5_1,
                                             tpccTools.aleaFloat(Float.valueOf("0.0000"), Float.valueOf("0.2000"), 4),
                                             TpccTools.WAREHOUSE_YTD,
                                             3001);
         txAwarePut(newDistrict);

         populateCustomers(id_warehouse, id_district);

         populateOrders(id_warehouse, id_district);
      }
   }

   protected void populateCustomers(int id_warehouse, int id_district) {
      if (id_warehouse < 0 || id_district < 0) {
         log.warn("Trying to populate Customer with a negative warehouse or district ID. skipping...");
         return;
      }

      logCustomerPopulation(id_warehouse, id_district, 1, TpccTools.NB_MAX_CUSTOMER);
      for (int i = 1; i <= TpccTools.NB_MAX_CUSTOMER; i++) {

         String c_last = c_last();
         Customer newCustomer = new Customer(id_warehouse,
                                             id_district,
                                             i,
                                             tpccTools.aleaChainec(8, 16),
                                             "OE",
                                             c_last,
                                             tpccTools.aleaChainec(10, 20),
                                             tpccTools.aleaChainec(10, 20),
                                             tpccTools.aleaChainec(10, 20),
                                             tpccTools.aleaChainel(2, 2),
                                             tpccTools.aleaChainen(4, 4) + TpccTools.CHAINE_5_1,
                                             tpccTools.aleaChainen(16, 16),
                                             new Date(System.currentTimeMillis()),
                                             (tpccTools.aleaNumber(1, 10) == 1) ? "BC" : "GC",
                                             500000.0,
                                             tpccTools.aleaDouble(0., 0.5, 4),
                                             -10.0,
                                             10.0,
                                             1,
                                             0,
                                             tpccTools.aleaChainec(300, 500));
         txAwarePut(newCustomer);

         CustomerLookup customerLookup = new CustomerLookup(c_last, id_warehouse, id_district);

         txAwareLoad(customerLookup);

         customerLookup.addId(i);

         txAwarePut(customerLookup);

         populateHistory(i, id_warehouse, id_district);
      }
   }

   protected void populateHistory(int id_customer, int id_warehouse, int id_district) {
      if (id_warehouse < 0 || id_district < 0 || id_customer < 0) {
         log.warn("Trying to populate Customer with a negative warehouse or district or customer ID. skipping...");
         return;
      }

      log.trace("Populating History for warehouse " + id_warehouse + ", district " + id_district + " and customer " +
                      id_customer);


      History newHistory = new History(id_customer, id_district, id_warehouse, id_district, id_warehouse,
                                       new Date(System.currentTimeMillis()), 10, tpccTools.aleaChainec(12, 24));
      txAwarePut(newHistory);
   }


   protected void populateOrders(int id_warehouse, int id_district) {
      if (id_warehouse < 0 || id_district < 0) {
         log.warn("Trying to populate Order with a negative warehouse or district ID. skipping...");
         return;
      }

      logOrderPopulation(id_warehouse, id_district, 1, TpccTools.NB_MAX_ORDER);
      this._new_order = false;
      for (int id_order = 1; id_order <= TpccTools.NB_MAX_ORDER; id_order++) {

         int o_ol_cnt = tpccTools.aleaNumber(5, 15);
         Date aDate = new Date((new java.util.Date()).getTime());

         Order newOrder = new Order(id_order,
                                    id_district,
                                    id_warehouse,
                                    generateSeqAlea(0, TpccTools.NB_MAX_CUSTOMER - 1),
                                    aDate,
                                    (id_order < TpccTools.LIMIT_ORDER) ? tpccTools.aleaNumber(1, 10) : 0,
                                    o_ol_cnt,
                                    1);


         txAwarePut(newOrder);

         populateOrderLines(id_warehouse, id_district, id_order, o_ol_cnt, aDate);

         if (id_order >= TpccTools.LIMIT_ORDER) {
            populateNewOrder(id_warehouse, id_district, id_order);
         }
      }
   }

   protected void populateOrderLines(int id_warehouse, int id_district, int id_order, int o_ol_cnt, Date aDate) {
      if (id_warehouse < 0 || id_district < 0) {
         log.warn("Trying to populate Order Lines with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating Orders Lines for warehouse " + id_warehouse + ", district " + id_district + " and order " +
                      id_order);
      for (int i = 0; i < o_ol_cnt; i++) {

         double amount;
         Date delivery_date;

         if (id_order >= TpccTools.LIMIT_ORDER) {
            amount = tpccTools.aleaDouble(0.01, 9999.99, 2);
            delivery_date = null;
         } else {
            amount = 0.0;
            delivery_date = aDate;
         }

         OrderLine newOrderLine = new OrderLine(id_order,
                                                id_district,
                                                id_warehouse,
                                                i,
                                                tpccTools.nonUniformRandom(getOL_I_ID(), TpccTools.A_OL_I_ID, 1L, TpccTools.NB_MAX_ITEM),
                                                id_warehouse,
                                                delivery_date,
                                                5,
                                                amount,
                                                tpccTools.aleaChainel(12, 24));
         txAwarePut(newOrderLine);
      }
   }

   protected void populateNewOrder(int id_warehouse, int id_district, int id_order) {
      if (id_warehouse < 0 || id_district < 0) {
         log.warn("Trying to populate New Order with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating New Order for warehouse " + id_warehouse + ", district " + id_district + " and order " +
                      id_order);

      NewOrder newNewOrder = new NewOrder(id_order, id_district, id_warehouse);

      txAwarePut(newNewOrder);
   }

   protected final boolean txAwarePut(DomainObject domainObject) {
      if (wrapper.isInTransaction()) {
         try {
            domainObject.store(wrapper, slaveIndex);
         } catch (Throwable throwable) {
            return false;
         }
      } else {
         boolean putDone = false;
         do {
            try {
               domainObject.store(wrapper, slaveIndex);
               putDone = true;
            } catch (Throwable e) {
               logErrorWhilePut(domainObject, e);
            }
         } while (!putDone);
      }
      return true;
   }

   protected final boolean txAwareLoad(DomainObject domainObject) {
      if (wrapper.isInTransaction()) {
         try {
            domainObject.load(wrapper);
         } catch (Throwable throwable) {
            return false;
         }
      } else {
         boolean loadDone = false;
         do {
            try {
               domainObject.load(wrapper);
               loadDone = true;
            } catch (Throwable e) {
               logErrorWhileGet(domainObject, e);
            }
         } while(!loadDone);
      }
      return true;
   }

   protected int generateSeqAlea(int deb, int fin) {
      if (!this._new_order) {
         for (int i = deb; i <= fin; i++) {
            this._seqIdCustomer[i] = i + 1;
         }
         this._new_order = true;
      }
      int rand;
      int alea;
      do {
         rand = (int) tpccTools.nonUniformRandom(getC_ID(), TpccTools.A_C_ID, deb, fin);
         alea = this._seqIdCustomer[rand];
      } while (alea == TpccTools.NULL_NUMBER);
      _seqIdCustomer[rand] = TpccTools.NULL_NUMBER;
      return alea;
   }

   protected void printMemoryInfo() {
      MemoryUsage u1 = this.memoryBean.getHeapMemoryUsage();
      log.info("Memory Statistics (Heap) - used=" + memString(u1.getUsed()) +
                     "; committed=" + memString(u1.getCommitted()));
      MemoryUsage u2 = this.memoryBean.getNonHeapMemoryUsage();
      log.info("Memory Statistics (NonHeap) - used=" + memString(u2.getUsed()) +
                     "; committed=" + memString(u2.getCommitted()));
   }

   protected void logStockPopulation(int warehouseID, long initID, long finishID) {
      log.debug("Populating Stock for Warehouse " + warehouseID + ", Items from " + initID + " to " + finishID);
   }

   protected void logOrderPopulation(int warehouseID, int districtID, long initID, long finishID) {
      log.debug("Populating Order for Warehouse " + warehouseID + " and District " + districtID +
                      " , Orders from " + initID + " to " + finishID);
   }

   protected void logCustomerPopulation(int warehouseID, int districtID, long initID, long finishID) {
      log.debug("Populating Customer for Warehouse " + warehouseID + " and District " + districtID +
                      " , Customer from " + initID + " to " + finishID);
   }

   protected void logItemsPopulation(long initID, long finishID) {
      log.debug("Populate Items from " + initID + " to " + finishID);
   }

   protected void logDistrictPopulation(int warehouse, long initId, long finishID) {
      log.debug("Populate Districts for Warehouse " + warehouse + ". Districts from " + initId + " to " + finishID);
   }

   private void logErrorWhilePut(Object object, Throwable throwable) {
      log.error("Error while trying to perform a put operation. Object is " + object +
                      ". Error is " + throwable.getLocalizedMessage() + ". Retrying...", throwable);
   }

   private void logErrorWhileGet(Object object, Throwable throwable) {
      log.error("Error while trying to perform a Get operation. Object is " + object +
                      ". Error is " + throwable.getLocalizedMessage() + ". Retrying...", throwable);
   }
}


