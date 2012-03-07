package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.tpcc.domain.Customer;
import org.radargun.tpcc.domain.CustomerLookup;
import org.radargun.tpcc.domain.Item;
import org.radargun.tpcc.domain.Order;
import org.radargun.tpcc.domain.Stock;

import java.util.Date;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Note: the code is not fully-engineered as it lacks some basic checks (for example on the number
 *  of threads).
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo      
 */
public class ThreadParallelTpccPopulation extends TpccPopulation{

   private static Log log = LogFactory.getLog(ThreadParallelTpccPopulation.class);
   private int parallelThreads = 4;
   private int elementsPerBlock = 100;  //items loaded per transaction

   public ThreadParallelTpccPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, int numSlaves,
                                       long cLastMask, long olIdMask, long cIdMask,
                                       int parallelThreads, int elementsPerBlock) {
      super(wrapper, numWarehouses, slaveIndex, numSlaves, cLastMask, olIdMask, cIdMask);
      this.parallelThreads = parallelThreads;
      this.elementsPerBlock = elementsPerBlock;
   }
      
   @Override
   protected void populateItem(){

      long init_id_item=1;
      long num_of_items=TpccTools.NB_MAX_ITEM;

      if(numSlaves>1){
         long remainder=TpccTools.NB_MAX_ITEM % numSlaves;
         num_of_items=(TpccTools.NB_MAX_ITEM-remainder)/numSlaves;

         init_id_item=(slaveIndex*num_of_items)+1;

         if(slaveIndex==numSlaves-1){
            num_of_items+=remainder;
         }
      }

      //Now compute the number of item per thread
      long thread_remainder = num_of_items % parallelThreads;
      long items_per_thread = (num_of_items - thread_remainder) / parallelThreads;

      long base = init_id_item;
      long itemToAdd;

      Thread[] waitFor = new Thread[parallelThreads];

      for(int i = 1; i<=parallelThreads; i++){
         itemToAdd = items_per_thread + ((i==parallelThreads)? thread_remainder:0);
         PopulateItemThread pit = new PopulateItemThread(base,base+itemToAdd-1, wrapper);
         waitFor[i-1] = pit;
         pit.start();
         base+=(itemToAdd);
      }

      waitForCompletion(waitFor);
   }

   @Override
   protected void populateStock(int id_warehouse){
      if (id_warehouse < 0) {
         return;
      }

      long init_id_item=1;
      long num_of_items=TpccTools.NB_MAX_ITEM;

      if(numSlaves>1){
         long remainder=TpccTools.NB_MAX_ITEM % numSlaves;
         num_of_items=(TpccTools.NB_MAX_ITEM-remainder)/numSlaves;

         init_id_item=(slaveIndex*num_of_items)+1;

         if(slaveIndex==numSlaves-1){
            num_of_items+=remainder;
         }
      }

      //Now, per thread
      long thread_remainder = num_of_items % parallelThreads;
      long items_per_thread = (num_of_items - thread_remainder) / parallelThreads;
      long base = init_id_item;
      long item_to_add;
      Thread[] waitFor = new Thread[parallelThreads];
      for(int i=1; i<=parallelThreads; i++){
         item_to_add = items_per_thread + ((i==parallelThreads)? thread_remainder:0);
         PopulateStockThread pst = new PopulateStockThread(base,base+item_to_add-1,id_warehouse,wrapper);
         waitFor[i-1] = pst;
         pst.start();

         base+=(item_to_add);
      }

      waitForCompletion(waitFor);
   }

   @Override
   protected void populateCustomers(int id_warehouse, int id_district){
      if (id_warehouse < 0 || id_district < 0) {
         return;
      }

      log.debug(" CUSTOMER " + id_warehouse + ", " + id_district);

      long thread_remainder = TpccTools.NB_MAX_CUSTOMER % parallelThreads;
      long items_per_thread = (TpccTools.NB_MAX_CUSTOMER - thread_remainder) / parallelThreads;

      long base = 1;
      long toAdd;
      ConcurrentHashMap<CustomerLookupQuadruple,Integer> lookupContentionAvoidance = new ConcurrentHashMap<CustomerLookupQuadruple, Integer>();
      Thread[] waitFor = new Thread[parallelThreads];
      for(int i=1; i<=parallelThreads; i++){
         toAdd = items_per_thread+((i==parallelThreads)? thread_remainder:0);
         PopulateCustomerThread pct = new PopulateCustomerThread(base, base+toAdd-1,id_warehouse,id_district,wrapper,lookupContentionAvoidance);
         waitFor[i-1] = pct;
         pct.start();

         base+=(toAdd);
      }

      waitForCompletion(waitFor);
      if(isBatchingEnabled()){
         populateCustomerLookup(lookupContentionAvoidance);
      }
   }

   private void populateCustomerLookup(ConcurrentHashMap<CustomerLookupQuadruple,Integer> map){
      log.debug("Populating customer lookup ");

      Vector<CustomerLookupQuadruple> vec_map = new Vector<CustomerLookupQuadruple>(map.keySet());
      long totalEntries = vec_map.size();
      log.debug("Size of the customer lookup is " + totalEntries);

      long remainder = totalEntries % parallelThreads;
      long items_per_thread = (totalEntries - remainder) / parallelThreads;

      long base = 0;
      long toAdd;

      Thread[] waitFor = new Thread[parallelThreads];

      for(int i=1; i<=parallelThreads;i++){
         toAdd = items_per_thread + ((i==parallelThreads)? remainder:0);
         //I put  -1   because we are starting from offset 0
         PopulateCustomerLookupThread pclt = new PopulateCustomerLookupThread(base,base+toAdd-1,vec_map,wrapper);
         waitFor[i-1] = pclt;
         pclt.start();
         base+=toAdd;
      }
      waitForCompletion(waitFor);
   }

   @Override
   protected void populateOrders(int id_warehouse, int id_district){
      this._new_order = false;

      long thread_remainder = TpccTools.NB_MAX_ORDER % parallelThreads;
      long items_per_thread = (TpccTools.NB_MAX_ORDER - thread_remainder) / parallelThreads;

      long base = 1;
      long toAdd;

      Thread[] waitFor = new Thread[parallelThreads];

      for(int i=1; i<=parallelThreads;i++){
         toAdd = items_per_thread + ((i==parallelThreads)? thread_remainder:0);
         PopulateOrderThread pot = new PopulateOrderThread(base,base+toAdd-1,id_warehouse,id_district,wrapper);
         waitFor[i-1] = pot;
         pot.start();
         base+=(toAdd); //inclusive
      }
      waitForCompletion(waitFor);
   }

   private boolean isBatchingEnabled(){
      return this.elementsPerBlock!=1;
   }

   private void waitForCompletion(Thread[] threads){
      try{
         for(int i=0;i<threads.length;i++){
            log.debug("Waiting for the end of Thread " + i);
            threads[i].join();
         }
         log.debug("All threads have finished! Movin' on");
      }
      catch(InterruptedException ie){
         ie.printStackTrace();
         System.exit(-1);
      }
   }

   // =======================  Population threads' classes  ======================

   private class PopulateOrderThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private int id_warehouse;
      private int id_district;
      private CacheWrapper wrapper;

      @Override
      public String toString() {
         return "PopulateOrderThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", id_warehouse=" + id_warehouse +
               ", id_district=" + id_district +
               '}';
      }

      public PopulateOrderThread(long l, long u, int w, int d, CacheWrapper c){
         this.lowerBound = l;
         this.upperBound = u;
         this.id_district = d;
         this.id_warehouse = w;
         this.wrapper = c;
      }

      public void run(){
         log.debug("Started " + this);
         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder) / elementsPerBlock;
         long elementsPerBatch = elementsPerBlock;

         if(numBatches ==0){
            numBatches=1;
            elementsPerBatch = 0;  //there is only the remainder ;)
         }

         long base = lowerBound;
         long toAdd;

         for(int j=1;j<=numBatches;j++){
            log.debug(this + " " + j + "-th batch out of " + numBatches);

            toAdd = elementsPerBatch + ((j==numBatches)? remainder:0);

            do {
               startTransactionIfNeeded();

               for(long id_order=base;id_order<base+toAdd;id_order++){

                  int o_ol_cnt = TpccTools.aleaNumber(5, 15);
                  Date aDate = new Date((new java.util.Date()).getTime());

                  Order newOrder= new Order(id_order,
                                            id_district,
                                            id_warehouse,
                                            generateSeqAlea(0, TpccTools.NB_MAX_CUSTOMER - 1),
                                            aDate,
                                            (id_order < TpccTools.LIMIT_ORDER)?TpccTools.aleaNumber(1, 10):0,
                                            o_ol_cnt,
                                            1);

                  this.stubbornPut(newOrder,wrapper);
                  populateOrderLines(id_warehouse, id_district, (int)id_order, o_ol_cnt, aDate);

                  if (id_order >= TpccTools.LIMIT_ORDER){
                     populateNewOrder(id_warehouse, id_district, (int)id_order);
                  }
               }
            } while (!endTransactionIfNeeded());
            base+=(toAdd);

         }
         log.debug("Ended "+this);
      }

      private void stubbornPut(Order o, CacheWrapper wrapper){
         boolean successful=false;
         while (!successful){
            try {
               o.store(wrapper);
               successful=true;
            } catch (Throwable e) {
               log.fatal(o, e);
            }
         }
      }
   }

   private class PopulateCustomerThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private CacheWrapper cacheWrapper;
      private int id_warehouse;
      private int id_district;
      private ConcurrentHashMap<CustomerLookupQuadruple,Integer> lookupContentionAvoidance;

      @Override
      public String toString() {
         return "PopulateCustomerThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", id_warehouse=" + id_warehouse +
               ", id_district=" + id_district +
               '}';
      }

      public PopulateCustomerThread(long lowerBound, long upperBound, int id_warehouse, int id_district,
                                    CacheWrapper wrapper,ConcurrentHashMap<CustomerLookupQuadruple,Integer> c){
         this.lowerBound = lowerBound;
         this.upperBound = upperBound;
         this.cacheWrapper = wrapper;
         this.id_district = id_district;
         this.id_warehouse = id_warehouse;
         this.lookupContentionAvoidance = c;
      }

      public void run(){
         log.debug("Started " + this);
         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder)  / elementsPerBlock;

         long base = lowerBound;
         long toAdd;
         long elementsPerBatch = elementsPerBlock;

         if(numBatches ==0){
            numBatches=1;
            elementsPerBatch = 0;  //there is only the remainder ;)
         }

         for(int j=1; j<=numBatches; j++){
            toAdd = elementsPerBatch + ((j==numBatches)? remainder:0);

            do {
               startTransactionIfNeeded();
               for(long i=base;i<base+toAdd;i++ ){

                  String c_last = c_last();
                  Customer newCustomer=new Customer(id_warehouse,
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
                                                    500000.0,
                                                    TpccTools.aleaDouble(0., 0.5, 4),
                                                    -10.0,
                                                    10.0,
                                                    1,
                                                    0,
                                                    TpccTools.aleaChainec(300, 500));

                  this.stubbornPut(newCustomer,wrapper);

                  if(isBatchingEnabled()){
                     CustomerLookupQuadruple clt = new CustomerLookupQuadruple(c_last,id_warehouse,id_district, i);
                     if(!this.lookupContentionAvoidance.containsKey(clt)){
                        this.lookupContentionAvoidance.put(clt,1);
                     }
                  } else{
                     CustomerLookup customerLookup = new CustomerLookup(c_last, id_warehouse, id_district);
                     stubbornLoad(customerLookup, cacheWrapper);
                     customerLookup.addId(i);
                     stubbornPut(customerLookup,cacheWrapper);
                  }

                  populateHistory((int) i, id_warehouse, id_district);
               }
            } while (!endTransactionIfNeeded());
            base+=(toAdd);
         }
         log.debug("Ended " + this);
      }

      private void stubbornPut(Customer c, CacheWrapper wrapper){
         boolean successful=false;
         while (!successful){
            try {
               c.store(wrapper);
               successful=true;
            } catch (Throwable e) {
               log.fatal(c, e);
            }
         }
      }

      private void stubbornPut(CustomerLookup c, CacheWrapper wrapper){
         boolean successful=false;
         while (!successful){
            try {
               c.store(wrapper);
               successful=true;
            } catch (Throwable e) {
               log.fatal(c, e);
            }
         }
      }

      private void stubbornLoad(CustomerLookup c, CacheWrapper wrapper){
         boolean successful=false;
         while (!successful){
            try {
               c.load(wrapper);
               successful=true;
            } catch (Throwable e) {
               log.fatal(c, e);
            }
         }
      }
   }

   private class PopulateItemThread extends Thread{

      private long lowerBound;
      private long upperBound;
      private CacheWrapper cacheWrapper;

      @Override
      public String toString() {
         return "PopulateItemThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               '}';
      }

      public PopulateItemThread(long low, long up, CacheWrapper c){
         this.lowerBound = low;
         this.upperBound = up;
         this.cacheWrapper = c;
      }

      public void run(){
         log.debug("Started" + this);
         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerBlock;
         long base = lowerBound;
         long toAdd;

         long elementsPerBatch = elementsPerBlock;

         if(numBatches ==0){
            numBatches=1;
            elementsPerBatch = 0;  //there is only the remainder ;)
         }

         for(long batch = 1; batch <=numBatches; batch++){
            toAdd = elementsPerBatch + ((batch==numBatches)? remainder:0);
            //Process a batch of elementsperBlock element

            do {
               startTransactionIfNeeded();
               for(long i=base; i<base+toAdd;i++){
                  Item newItem = new Item(i,
                                          TpccTools.aleaNumber(1, 10000),
                                          TpccTools.aleaChainec(14, 24),
                                          TpccTools.aleaFloat(1, 100, 2),
                                          TpccTools.sData());
                  this.stubbornPut(newItem, this.cacheWrapper);
               }
            } while (!endTransactionIfNeeded());
            base+=(toAdd);
         }
         log.debug("Ended " + this);
      }


      private void stubbornPut(Item newItem, CacheWrapper wrapper){
         boolean successful=false;
         while (!successful){
            try {
               newItem.store(wrapper);
               successful=true;
            } catch (Throwable e) {
               log.fatal(newItem, e);
            }
         }
      }
   }

   private class PopulateStockThread extends Thread{

      private long lowerBound;
      private long upperBound;
      private int id_warehouse;
      private CacheWrapper cacheWrapper;

      @Override
      public String toString() {
         return "PopulateStockThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", id_warehouse=" + id_warehouse +
               '}';
      }

      public PopulateStockThread(long low, long up, int id_warehouse, CacheWrapper c){
         this.lowerBound = low;
         this.upperBound = up;
         this.cacheWrapper = c;
         this.id_warehouse = id_warehouse;
      }

      public void run(){
         log.debug("Started " + this);

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerBlock;
         long base = lowerBound;
         long toAdd;

         long elementsPerBatch = elementsPerBlock;

         if(numBatches ==0){
            numBatches=1;
            elementsPerBatch = 0;  //there is only the remainder ;)
         }

         for(long batch = 1; batch <=numBatches; batch++){
            toAdd = elementsPerBatch + ((batch==numBatches)? remainder:0);
            //Process a batch of elementsperBlock element

            do {
               startTransactionIfNeeded();
               for(long i=base; i<base+toAdd;i++){
                  Stock newStock=new Stock(i,
                                           this.id_warehouse,
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
                  this.stubbornPut(newStock,this.cacheWrapper);
               }
            } while (!endTransactionIfNeeded());
            base+=(toAdd);
         }
         log.debug("Ended " +this);
      }


      private void stubbornPut(Stock newStock, CacheWrapper wrapper){
         boolean successful=false;
         while (!successful){
            try {
               newStock.store(wrapper);
               successful = true;
            } catch (Throwable e) {
               log.fatal(newStock, e);
            }
         }
      }
   }

   private class PopulateCustomerLookupThread extends Thread{
      private Vector<CustomerLookupQuadruple> vector;
      private long lowerBound;
      private long upperBound;
      private CacheWrapper wrapper;

      @Override
      public String toString() {
         return "PopulateCustomerLookupThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               '}';
      }

      public PopulateCustomerLookupThread(long l, long u, Vector<CustomerLookupQuadruple> v, CacheWrapper c){
         this.vector = v;
         this.lowerBound = l;
         this.upperBound = u;
         this.wrapper = c;
      }

      public void run(){
         log.debug("Starting " + this);
         //I have to put +1  because it's inclusive
         long remainder = (upperBound - lowerBound  +1) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound + 1 - remainder ) / elementsPerBlock;
         long base = lowerBound;
         long toAdd;

         long elementsPerBatch = elementsPerBlock;

         if(numBatches ==0){
            numBatches=1;
            elementsPerBatch = 0;  //there is only the remainder ;)
         }

         for(long batch = 1; batch <=numBatches; batch++){
            toAdd = elementsPerBatch + ((batch==numBatches)? remainder:0);

            do {
               startTransactionIfNeeded();
               for(long i=base; i<base+toAdd;i++){

                  CustomerLookupQuadruple clq = this.vector.get((int)i);
                  CustomerLookup customerLookup = new CustomerLookup(clq.c_last, clq.id_warehouse, clq.id_district);
                  this.stubbornLoad(customerLookup, wrapper);
                  customerLookup.addId(clq.id_customer);
                  this.stubbornPut(customerLookup,wrapper);
               }
            } while (!endTransactionIfNeeded());
            base+=toAdd;
         }
         log.debug("Ending " + this);
      }

      private void stubbornPut(CustomerLookup c, CacheWrapper wrapper){
         boolean successful=false;
         while (!successful){
            try {
               c.store(wrapper);
               successful=true;
            } catch (Throwable e) {
               log.fatal(c, e);
            }
         }
      }

      private void stubbornLoad(CustomerLookup c, CacheWrapper wrapper){
         boolean successful=false;
         while (!successful){
            try {
               c.load(wrapper);
               successful=true;
            } catch (Throwable e) {
               log.fatal(c, e);
            }
         }
      }
   }

   private class CustomerLookupQuadruple {
      private String c_last;
      private int id_warehouse;
      private int id_district;
      private long id_customer;


      public CustomerLookupQuadruple(String c, int w, int d, long i){
         this.c_last = c;
         this.id_warehouse = w;
         this.id_district = d;
         this.id_customer = i;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CustomerLookupQuadruple that = (CustomerLookupQuadruple) o;
         //The customer id does not count!!! it's not part of the key
         //if (id_customer != that.id_customer) return false;
         return id_district == that.id_district 
               && id_warehouse == that.id_warehouse 
               && !(c_last != null ? !c_last.equals(that.c_last) : that.c_last != null);
      }

      @Override
      public int hashCode() {
         int result = c_last != null ? c_last.hashCode() : 0;
         result = 31 * result + id_warehouse;
         result = 31 * result + id_district;
         //I don't need id_customer since it's not part of a customerLookup's key
         //result = 31 * result + (int)id_customer;
         return result;
      }

      @Override
      public String toString() {
         return "CustomerLookupQuadruple{" +
               "c_last='" + c_last + '\'' +
               ", id_warehouse=" + id_warehouse +
               ", id_district=" + id_district +
               ", id_customer=" + id_customer +
               '}';
      }
   }

   private void startTransactionIfNeeded() {
      if (isBatchingEnabled()) {
         wrapper.startTransaction();
      }
   }

   private boolean endTransactionIfNeeded() {
      if (!isBatchingEnabled()) {
         return true;
      }
      try {
         wrapper.endTransaction(true);
      } catch (Throwable t) {
         log.warn("Error committing transaction. retrying");
         sleepRandomly();
         return false;
      }
      return true;
   }

   private void sleepRandomly() {
      try {
         Random r = new Random();
         long sleepFor;
         do {
            sleepFor = r.nextLong();
         } while (sleepFor <= 0);
         Thread.sleep(sleepFor % 60000);
      } catch (InterruptedException e) {
         //no-op
      }
   }
}
