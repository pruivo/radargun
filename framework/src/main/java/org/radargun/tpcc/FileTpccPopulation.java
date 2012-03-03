package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.tpcc.domain.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public class FileTpccPopulation {

   private static final String FILE_NAME = "__tpcc_population_data__";

   private static Log log = LogFactory.getLog(FileTpccPopulation.class);

   private long POP_C_LAST = TpccTools.NULL_NUMBER;

   private long POP_C_ID = TpccTools.NULL_NUMBER;

   private long POP_OL_I_ID = TpccTools.NULL_NUMBER;

   private boolean _new_order = false;

   private int _seqIdCustomer[];

   private MemoryMXBean memoryBean;

   private int numWarehouses;

   private long cLastMask;

   private long olIdMask;

   private long cIdMask;

   private String folder;

   private Map<String, CustomerLookup> customerLookupMap;

   public FileTpccPopulation(int numWarehouses, long cLastMask, long olIdMask, long cIdMask, String folder) {
      this.folder = folder;
      this._seqIdCustomer = new int[TpccTools.NB_MAX_CUSTOMER];
      this.memoryBean = ManagementFactory.getMemoryMXBean();
      this.numWarehouses = numWarehouses;
      this.cLastMask = cLastMask;
      this.olIdMask = olIdMask;
      this.cIdMask = cIdMask;
      this.customerLookupMap = new HashMap<String, CustomerLookup>();
   }

   public long populateToFile() throws IOException {
      CounterObjectOutput out = createObjectOutputStream(FILE_NAME);
      init();
      initializeToolsParameters(out);
      populateItem(out);
      populateWarehouses(out);
      populateCustomerLookup(out);
      return out.getCounter();
   }

   public void populateFromFile(int numberOfSlaves, CacheWrapper wrapper)
         throws IOException, ClassNotFoundException {
      init();
      File f = new File(folder);
      if (!f.isDirectory()) {
         throw new IOException(folder + " is not a directory");
      }


      ObjectInputStream ois = createObjectInputStream(FILE_NAME);

      long c_c_last = ois.readLong();
      long c_c_id = ois.readLong();
      long c_ol_i_id = ois.readLong();

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

      while (ois.available() > 0) {
         DomainObject domainObject = (DomainObject) ois.readObject();

         successful = false;
         while (!successful) {
            try {
               domainObject.store(wrapper);
               successful = true;
            } catch (Throwable e) {
               log.warn(e);
            }
         }
      }
   }

   private void populateCustomerLookup(CounterObjectOutput out) throws IOException {
      for (CustomerLookup customerLookup : customerLookupMap.values()) {
         out.writeObject(customerLookup);
      }
      out.flush();
   }


   private CounterObjectOutput createObjectOutputStream(String fileName) throws IOException {
      return new CounterObjectOutput(new ObjectOutputStream(new FileOutputStream(folder + fileName)));
   }

   private ObjectInputStream createObjectInputStream(String fileName) throws IOException {
      return new ObjectInputStream(new FileInputStream(folder + fileName));
   }

   private void init() {
      TpccTools.NB_WAREHOUSES = this.numWarehouses;
      TpccTools.A_C_LAST = this.cLastMask;
      TpccTools.A_OL_I_ID = this.olIdMask;
      TpccTools.A_C_ID = this.cIdMask;
   }

   public void initializeToolsParameters(CounterObjectOutput out) throws IOException {
      init();

      long c_c_last = TpccTools.randomNumber(0, TpccTools.A_C_LAST);
      long c_c_id = TpccTools.randomNumber(0, TpccTools.A_C_ID);
      long c_ol_i_id = TpccTools.randomNumber(0, TpccTools.A_OL_I_ID);

      out.writeLong(c_c_last);
      out.writeLong(c_c_id);
      out.writeLong(c_ol_i_id);
      out.flush();

   }

   public String c_last() {
      String c_last = "";
      long number = TpccTools.nonUniformRandom(getC_LAST(), TpccTools.A_C_LAST, TpccTools.MIN_C_LAST, TpccTools.MAX_C_LAST);
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
         POP_C_LAST = TpccTools.randomNumber(TpccTools.MIN_C_LAST, TpccTools.A_C_LAST);
      }
      return POP_C_LAST;
   }

   public long getC_ID() {
      if (POP_C_ID == TpccTools.NULL_NUMBER) {
         POP_C_ID = TpccTools.randomNumber(0, TpccTools.A_C_ID);
      }
      return POP_C_ID;
   }

   public long getOL_I_ID() {
      if (POP_OL_I_ID == TpccTools.NULL_NUMBER) {
         POP_OL_I_ID = TpccTools.randomNumber(0, TpccTools.A_OL_I_ID);
      }
      return POP_OL_I_ID;
   }


   private void populateItem(CounterObjectOutput out) throws IOException {
      log.info("populate items");

      long init_id_item = 1;
      long num_of_items = TpccTools.NB_MAX_ITEM;

      log.info(" ITEM - ids=" + init_id_item + ",...," + (init_id_item - 1 + num_of_items));
      for (long i = init_id_item; i <= (init_id_item - 1 + num_of_items); i++) {

         Item newItem = new Item(i, TpccTools.aleaNumber(1, 10000), TpccTools.aleaChainec(14, 24), TpccTools.aleaFloat(1, 100, 2), TpccTools.sData());

         out.writeObject(newItem);
      }

      out.flush();
      MemoryUsage u1 = this.memoryBean.getHeapMemoryUsage();
      log.info("Memory Statistics (Heap) - used=" + u1.getUsed() + " bytes; committed=" + u1.getCommitted() + " bytes");
      MemoryUsage u2 = this.memoryBean.getNonHeapMemoryUsage();
      log.info("Memory Statistics (NonHeap) - used=" + u2.getUsed() + " bytes; committed=" + u2.getCommitted() + " bytes");
   }

   private void populateWarehouses(CounterObjectOutput out) throws IOException {
      log.info("populate warehouses");

      if (this.numWarehouses > 0) {
         for (int i = 1; i <= this.numWarehouses; i++) {
            log.info(" WAREHOUSE " + i);

            Warehouse newWarehouse = new Warehouse(i,
                                                   TpccTools.aleaChainec(6, 10),
                                                   TpccTools.aleaChainec(10, 20), TpccTools.aleaChainec(10, 20),
                                                   TpccTools.aleaChainec(10, 20), TpccTools.aleaChainel(2, 2),
                                                   TpccTools.aleaChainen(4, 4) + TpccTools.CHAINE_5_1,
                                                   TpccTools.aleaFloat(Float.valueOf("0.0000"), Float.valueOf("0.2000"), 4),
                                                   TpccTools.WAREHOUSE_YTD);

            out.writeObject(newWarehouse);
            out.flush();

            populateStock(i, out);
            populateDistricts(i, out);

            MemoryUsage u1 = this.memoryBean.getHeapMemoryUsage();
            log.info("Memory Statistics (Heap) - used=" + u1.getUsed() + " bytes; committed=" + u1.getCommitted() + " bytes");
            MemoryUsage u2 = this.memoryBean.getNonHeapMemoryUsage();
            log.info("Memory Statistics (NonHeap) - used=" + u2.getUsed() + " bytes; committed=" + u2.getCommitted() + " bytes");
         }
      }
   }

   private void populateStock(int id_warehouse, CounterObjectOutput out) throws IOException {
      log.info("populate stocks");
      if (id_warehouse < 0) return;


      long init_id_item = 1;
      long num_of_items = TpccTools.NB_MAX_ITEM;

      log.info(" STOCK for Warehouse " + id_warehouse + " - ITEMS=" + init_id_item + ",...," + (init_id_item - 1 + num_of_items));
      for (long i = init_id_item; i <= (init_id_item - 1 + num_of_items); i++) {

         Stock newStock = new Stock(i,
                                    id_warehouse,
                                    TpccTools.aleaNumber(10, 100),
                                    TpccTools.aleaChainel(24, 24),
                                    TpccTools.aleaChainel(24, 24),
                                    TpccTools.aleaChainel(24, 24),
                                    TpccTools.aleaChainel(24, 24),
                                    TpccTools.aleaChainel(24, 24),
                                    TpccTools.aleaChainel(24, 24),
                                    TpccTools.aleaChainel(24, 24),
                                    TpccTools.aleaChainel(24, 24),
                                    TpccTools.aleaChainel(24, 24),
                                    TpccTools.aleaChainel(24, 24),
                                    0,
                                    0,
                                    0,
                                    TpccTools.sData());

         out.writeObject(newStock);
      }

      out.flush();
   }

   private void populateDistricts(int id_warehouse, CounterObjectOutput out) throws IOException {
      log.info("populate districts");
      if (id_warehouse < 0) return;

      int init_id_district = 1;
      int num_of_districts = TpccTools.NB_MAX_DISTRICT;

      for (int id_district = init_id_district; id_district <= (init_id_district - 1 + num_of_districts); id_district++) {
         log.info(" DISTRICT " + id_district);

         District newDistrict = new District(id_warehouse,
                                             id_district,
                                             TpccTools.aleaChainec(6, 10),
                                             TpccTools.aleaChainec(10, 20),
                                             TpccTools.aleaChainec(10, 20),
                                             TpccTools.aleaChainec(10, 20),
                                             TpccTools.aleaChainel(2, 2),
                                             TpccTools.aleaChainen(4, 4) + TpccTools.CHAINE_5_1,
                                             TpccTools.aleaFloat(Float.valueOf("0.0000"), Float.valueOf("0.2000"), 4),
                                             TpccTools.WAREHOUSE_YTD,
                                             3001);

         out.writeObject(newDistrict);
         out.flush();

         populateCustomers(id_warehouse, id_district, out);
         populateOrders(id_warehouse, id_district, out);
      }
   }

   private void populateCustomers(int id_warehouse, int id_district, CounterObjectOutput out) throws IOException {
      log.info("populate customer");
      if (id_warehouse < 0 || id_district < 0) return;

      log.info(" CUSTOMER " + id_warehouse + ", " + id_district);

      String c_last;
      for (int i = 1; i <= TpccTools.NB_MAX_CUSTOMER; i++) {

         c_last = c_last();

         Customer newCustomer = new Customer(id_warehouse,
                                             id_district,
                                             i,
                                             TpccTools.aleaChainec(8, 16),
                                             "OE",
                                             c_last,
                                             TpccTools.aleaChainec(10, 20),
                                             TpccTools.aleaChainec(10, 20),
                                             TpccTools.aleaChainec(10, 20),
                                             TpccTools.aleaChainel(2, 2),
                                             TpccTools.aleaChainen(4, 4) + TpccTools.CHAINE_5_1,
                                             TpccTools.aleaChainen(16, 16),
                                             new Date(System.currentTimeMillis()),
                                             (TpccTools.aleaNumber(1, 10) == 1) ? "BC" : "GC",
                                             500000.0, TpccTools.aleaDouble(0., 0.5, 4), -10.0, 10.0, 1, 0, TpccTools.aleaChainec(300, 500));


         out.writeObject(newCustomer);
         out.flush();

         CustomerLookup customerLookup = new CustomerLookup(c_last, id_warehouse, id_district);
         customerLookup.load(customerLookupMap).addId(i);
         customerLookup.save(customerLookupMap);

         populateHistory(i, id_warehouse, id_district, out);
      }
   }

   private void populateHistory(int id_customer, int id_warehouse, int id_district, CounterObjectOutput out) throws IOException {
      //log.info("populate history");
      if (id_customer < 0 || id_warehouse < 0 || id_district < 0) return;


      History newHistory = new History(id_customer, id_district, id_warehouse, id_district, id_warehouse,
                                       new Date(System.currentTimeMillis()), 10, TpccTools.aleaChainec(12, 24));

      out.writeObject(newHistory);
      out.flush();
   }



   private void populateOrders(int id_warehouse, int id_district, CounterObjectOutput out) throws IOException {
      log.info("populate order");
      this._new_order = false;
      log.info(" ORDER " + id_warehouse + ", " + id_district);

      for (int id_order = 1; id_order <= TpccTools.NB_MAX_ORDER; id_order++) {

         int o_ol_cnt = TpccTools.aleaNumber(5, 15);
         Date aDate = new Date((new Date()).getTime());

         Order newOrder = new Order(id_order,
                                    id_district,
                                    id_warehouse,
                                    generateSeqAlea(0, TpccTools.NB_MAX_CUSTOMER - 1),
                                    aDate,
                                    (id_order < TpccTools.LIMIT_ORDER) ? TpccTools.aleaNumber(1, 10) : 0,
                                    o_ol_cnt,
                                    1);

         out.writeObject(newOrder);
         out.flush();

         populateOrderLines(id_warehouse, id_district, id_order, o_ol_cnt, aDate, out);

         if (id_order >= TpccTools.LIMIT_ORDER) populateNewOrder(id_warehouse, id_district, id_order, out);
      }
   }

   private void populateOrderLines(int id_warehouse, int id_district, int id_order, int o_ol_cnt, Date aDate, CounterObjectOutput out) throws IOException {
      //log.info("populate order line");

      for (int i = 0; i < o_ol_cnt; i++) {

         double amount;
         Date delivery_date;

         if (id_order >= TpccTools.LIMIT_ORDER) {
            amount = TpccTools.aleaDouble(0.01, 9999.99, 2);
            delivery_date = null;
         } else {
            amount = 0.0;
            delivery_date = aDate;
         }


         OrderLine newOrderLine = new OrderLine(id_order,
                                                id_district,
                                                id_warehouse,
                                                i,
                                                TpccTools.nonUniformRandom(getOL_I_ID(), TpccTools.A_OL_I_ID, 1L, TpccTools.NB_MAX_ITEM),
                                                id_warehouse,
                                                delivery_date,
                                                5,
                                                amount,
                                                TpccTools.aleaChainel(12, 24));

         out.writeObject(newOrderLine);
      }
      out.flush();
   }

   private void populateNewOrder(int id_warehouse, int id_district, int id_order, CounterObjectOutput out) throws IOException {
      //log.info("populate new order");

      NewOrder newNewOrder = new NewOrder(id_order, id_district, id_warehouse);

      out.writeObject(newNewOrder);
      out.flush();
   }

   private int generateSeqAlea(int deb, int fin) {
      if (!this._new_order) {
         for (int i = deb; i <= fin; i++) {
            this._seqIdCustomer[i] = i + 1;
         }
         this._new_order = true;
      }
      int rand = 0;
      int alea = 0;
      do {
         rand = (int) TpccTools.nonUniformRandom(getC_ID(), TpccTools.A_C_ID, deb, fin);
         alea = this._seqIdCustomer[rand];
      } while (alea == TpccTools.NULL_NUMBER);
      _seqIdCustomer[rand] = TpccTools.NULL_NUMBER;
      return alea;
   }
}



