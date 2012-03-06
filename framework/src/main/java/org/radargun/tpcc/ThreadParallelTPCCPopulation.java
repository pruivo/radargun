package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;

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
public class ThreadParallelTPCCPopulation extends TPCCPopulation{

   private static Log log = LogFactory.getLog(ThreadParallelTPCCPopulation.class);
   private int parallelThreads = 4;
   private int elementsPerBlock = 100;  //items loaded per transaction




   public ThreadParallelTPCCPopulation(CacheWrapper c, int slaveIndex, int numSlaves, boolean light, int parallelThreads, int batchLevel){
      super(c,slaveIndex,numSlaves,light);
      this.parallelThreads = parallelThreads;
      this.elementsPerBlock = batchLevel;

   }

   protected void populate_item(){

      long init_id_item=1;
      long num_of_items=TPCCTools.NB_MAX_ITEM;

      if(numSlaves>1){
         long remainder=TPCCTools.NB_MAX_ITEM % numSlaves;
         num_of_items=(TPCCTools.NB_MAX_ITEM-remainder)/numSlaves;


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

   protected void populate_stock(int id_warehouse){
      if (id_warehouse < 0)
         return;


      long init_id_item=1;
      long num_of_items=TPCCTools.NB_MAX_ITEM;

      if(numSlaves>1){
         long remainder=TPCCTools.NB_MAX_ITEM % numSlaves;
         num_of_items=(TPCCTools.NB_MAX_ITEM-remainder)/numSlaves;


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

   protected void populate_customer(int id_warehouse, int id_district){
      if (id_warehouse < 0 || id_district < 0);
      log.debug(" CUSTOMER " + id_warehouse + ", " + id_district);

      long thread_remainder = TPCCTools.NB_MAX_CUSTOMER % parallelThreads;
      long items_per_thread = (TPCCTools.NB_MAX_CUSTOMER - thread_remainder) / parallelThreads;

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
         populate_customer_lookup(lookupContentionAvoidance);
      }

   }

   private void populate_customer_lookup(ConcurrentHashMap<CustomerLookupQuadruple,Integer> map){
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

   /*
   private void populate_customer_lookup(ConcurrentHashMap<CustomerLookupQuadruple,Integer> map){
       log.debug("Populating customer lookup ");
       int count = 1;

       for(CustomerLookupQuadruple clq : map.keySet()){

               if(isBatchingEnabled() && (count%elementsPerBlock==0 || count== map.size()))
                   wrapper.startTransaction();

               CustomerLookup customerLookup = new CustomerLookup(clq.c_last, clq.id_warehouse, clq.id_district);
               this.stubbornLoad(customerLookup, wrapper);
               customerLookup.addId(clq.id_customer);
               this.stubbornPut(customerLookup,wrapper);

               if(isBatchingEnabled() && (count%elementsPerBlock==0 || count== map.size())){
                   try{
                       wrapper.endTransaction(true);
                   }
                   catch(RollbackException re){
                       re.printStackTrace();
                       System.exit(-1);
                   }
               }
           count++;

       }
       log.debug("Population of customer lookup ended");
   }
   */


   protected void populate_order(int id_warehouse, int id_district){
      this._new_order = false;

      long thread_remainder = TPCCTools.NB_MAX_ORDER % parallelThreads;
      long items_per_thread = (TPCCTools.NB_MAX_ORDER - thread_remainder) / parallelThreads;

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






   /*
       Pupulator threads' classes
   */

   private class PopulateOrderThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private int id_warehouse;
      private int id_district;
      private CacheWrapper wrapper;

      public String toString(){
         return "PopulateOrderThread Node "+slaveIndex+" lowerBound "+lowerBound+" upperBound "+upperBound+" warehouse "+id_warehouse+" district "+id_district;
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

                  int o_ol_cnt = TPCCTools.alea_number(5, 15);
                  Date aDate = new Date((new java.util.Date()).getTime());

                  Order newOrder= new Order(id_order,
                                            id_district,
                                            id_warehouse,
                                            generate_seq_alea(0, TPCCTools.NB_MAX_CUSTOMER-1),
                                            aDate,
                                            (id_order < TPCCTools.LIMIT_ORDER)?TPCCTools.alea_number(1, 10):0,
                                            o_ol_cnt,
                                            1);

                  this.stubbornPut(newOrder,wrapper);
                  populate_order_line(id_warehouse, id_district, (int)id_order, o_ol_cnt, aDate);

                  if (id_order >= TPCCTools.LIMIT_ORDER){
                     populate_new_order(id_warehouse, id_district, (int)id_order);
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
               log.fatal(e);
               log.fatal(o);
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

      public String toString(){
         return "PopulateCustomerThread "+ +slaveIndex+" lowerBound "+lowerBound+" upperBound "+upperBound+" warehouse "+id_warehouse+" district "+id_district;
      }

      public PopulateCustomerThread(long lowerBound, long upperBound, int id_warehouse, int id_district, CacheWrapper wrapper,ConcurrentHashMap c){

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
                  Customer newCustomer;
                  if(!light){
                     newCustomer=new Customer(id_warehouse,
                                              id_district,
                                              i,
                                              TPCCTools.alea_chainec(8, 16),
                                              "OE",
                                              c_last,
                                              TPCCTools.alea_chainec(10, 20),
                                              TPCCTools.alea_chainec(10, 20),
                                              TPCCTools.alea_chainec(10, 20),
                                              TPCCTools.alea_chainel(2, 2),
                                              TPCCTools.alea_chainen(4, 4) + TPCCTools.CHAINE_5_1,
                                              TPCCTools.alea_chainen(16, 16),
                                              new Date(System.currentTimeMillis()),
                                              (TPCCTools.alea_number(1, 10) == 1) ? "BC" : "GC",
                                              500000.0, TPCCTools.alea_double(0., 0.5, 4), -10.0, 10.0, 1, 0, TPCCTools.alea_chainec(300, 500));
                  }
                  else{
                     newCustomer=new Customer(id_warehouse,
                                              id_district,
                                              i,
                                              null,
                                              null,
                                              c_last,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              new Date(System.currentTimeMillis()),
                                              (TPCCTools.alea_number(1, 10) == 1) ? "BC" : "GC",
                                              500000.0, TPCCTools.alea_double(0., 0.5, 4), -10.0, 10.0, 1, 0, TPCCTools.alea_chainec(300, 500));
                  }
                  this.stubbornPut(newCustomer,wrapper);

                  if(isBatchingEnabled()){
                     CustomerLookupQuadruple clt = new CustomerLookupQuadruple(c_last,id_warehouse,id_district, i);
                     if(!this.lookupContentionAvoidance.containsKey(clt)){
                        this.lookupContentionAvoidance.put(clt,1);
                     }
                  }
                  else{
                     CustomerLookup customerLookup = new CustomerLookup(c_last, id_warehouse, id_district);
                     stubbornLoad(customerLookup, cacheWrapper);
                     customerLookup.addId(i);
                     stubbornPut(customerLookup,cacheWrapper);
                  }

                  populate_history((int)i, id_warehouse, id_district);
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
               log.fatal(e);
               log.fatal(c);
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
               log.fatal(e);
               log.fatal(c);
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
               log.fatal(e);
               log.fatal(c);
            }
         }
      }

   }

   private class PopulateItemThread extends Thread{

      private long lowerBound;
      private long upperBound;
      private CacheWrapper cacheWrapper;

      @Override
      public String toString(){
         return "PopulateItemThread "+slaveIndex+" lowerBound "+lowerBound+" upperBound "+upperBound;
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
                                          TPCCTools.alea_number(1, 10000),
                                          TPCCTools.alea_chainec(14, 24),
                                          TPCCTools.alea_float(1, 100, 2),
                                          TPCCTools.s_data());
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
               //System.out.println("Inserita "+newItem.getKey());
            } catch (Throwable e) {
               log.fatal(e);
               log.fatal(newItem);
            }
         }

      }
   }

   private class PopulateStockThread extends Thread{

      private long lowerBound;
      private long upperBound;
      private int id_wharehouse;
      private CacheWrapper cacheWrapper;

      @Override
      public String toString(){
         return "node "+slaveIndex+" lowerBound "+lowerBound+" upperBound "+upperBound+" Warehouse "+id_wharehouse;
      }

      public PopulateStockThread(long low, long up, int id_wharehouse, CacheWrapper c){
         this.lowerBound = low;
         this.upperBound = up;
         this.cacheWrapper = c;
         this.id_wharehouse = id_wharehouse;


      }

      public void run(){
         log.debug("Started " + this);
         //
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
                                           this.id_wharehouse,
                                           TPCCTools.alea_number(10, 100),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           0,
                                           0,
                                           0,
                                           TPCCTools.s_data());
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
               log.fatal(e);
            }
         }

      }


   }

   private class PopulateCustomerLookupThread extends Thread{
      private Vector<CustomerLookupQuadruple> vector;
      private long lowerBound;
      private long upperBound;
      private CacheWrapper wrapper;

      public String toString(){
         return "PopulateCustomerLookupThread lowerBound "+lowerBound+" upperBound "+upperBound;
      }

      public PopulateCustomerLookupThread(long l, long u, Vector v, CacheWrapper c){
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
               log.fatal(e);
               log.fatal(c);
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
               log.fatal(e);
               log.fatal(c);
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
         if (id_district != that.id_district) return false;
         if (id_warehouse != that.id_warehouse) return false;
         if (c_last != null ? !c_last.equals(that.c_last) : that.c_last != null) return false;

         return true;
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

      public String getC_last() {
         return c_last;
      }

      public int getId_warehouse() {
         return id_warehouse;
      }

      public int getId_district() {
         return id_district;
      }

      public long getId_customer() {
         return id_customer;
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
         Thread.sleep(sleepFor);
      } catch (InterruptedException e) {
         //no-op
      }
   }

}
