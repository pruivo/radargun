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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Note: the code is not fully-engineered as it lacks some basic checks (for example on the number
 *  of threads).
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo      
 */
public class ThreadParallelTpccPopulation extends TpccPopulation{

   private static Log log = LogFactory.getLog(ThreadParallelTpccPopulation.class);
   private static final long MAX_SLEEP_BEFORE_RETRY = 30000; //30 seconds

   private int parallelThreads = 4;
   private int elementsPerBlock = 100;  //items loaded per transaction
   private AtomicLong waitingPeriod;

   public ThreadParallelTpccPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, int numSlaves,
                                       long cLastMask, long olIdMask, long cIdMask,
                                       int parallelThreads, int elementsPerBlock) {
      super(wrapper, numWarehouses, slaveIndex, numSlaves, cLastMask, olIdMask, cIdMask);
      this.parallelThreads = parallelThreads;
      this.elementsPerBlock = elementsPerBlock;

      if (this.parallelThreads <= 0) {
         log.warn("Parallel threads must be greater than zero. disabling parallel population");
         this.parallelThreads = 1;
      }
      if (this.elementsPerBlock <= 0) {
         log.warn("Batch level must be greater than zero. disabling batching level");
         this.elementsPerBlock = 1;
      }

      this.waitingPeriod = new AtomicLong(0);
   }

   @Override
   protected void populateItem(){
      log.trace("Populating Items");

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

      performMultiThreadPopulation(init_id_item, num_of_items, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateItemThread(lowerBound, upperBound);
         }
      });
   }

   @Override
   protected void populateStock(final int id_warehouse){
      if (id_warehouse < 0) {
         log.warn("Trying to populate Stock for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating Stock for warehouse " + id_warehouse);

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

      performMultiThreadPopulation(init_id_item, num_of_items, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateStockThread(lowerBound, upperBound, id_warehouse);
         }
      });
   }

   @Override
   protected void populateCustomers(final int id_warehouse, final int id_district){
      if (id_warehouse < 0 || id_district < 0) {
         log.warn("Trying to populate Customer with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating Customers for warehouse " + id_warehouse + " and district " + id_district);

      final ConcurrentHashMap<CustomerLookupQuadruple,Integer> lookupContentionAvoidance =
            new ConcurrentHashMap<CustomerLookupQuadruple, Integer>();

      performMultiThreadPopulation(1, TpccTools.NB_MAX_CUSTOMER, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateCustomerThread(lowerBound, upperBound, id_warehouse, id_district, lookupContentionAvoidance);
         }
      });

      if(isBatchingEnabled()){
         populateCustomerLookup(lookupContentionAvoidance);
      }
   }

   protected void populateCustomerLookup(ConcurrentHashMap<CustomerLookupQuadruple, Integer> map){
      log.trace("Populating customer lookup ");

      final Vector<CustomerLookupQuadruple> vec_map = new Vector<CustomerLookupQuadruple>(map.keySet());
      long totalEntries = vec_map.size();

      log.trace("Populating customer lookup. Size is " + totalEntries);

      performMultiThreadPopulation(0, totalEntries, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateCustomerLookupThread(lowerBound, upperBound, vec_map);
         }
      });
   }

   @Override
   protected void populateOrders(final int id_warehouse, final int id_district){
      if (id_warehouse < 0 || id_district < 0) {
         log.warn("Trying to populate Order with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating Orders for warehouse " + id_warehouse + " and district " + id_district);
      this._new_order = false;

      performMultiThreadPopulation(1, TpccTools.NB_MAX_ORDER, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateOrderThread(lowerBound, upperBound, id_warehouse, id_district);
         }
      });
   }

   /*
    * ######################################### POPULATING THREADS ################################
    */

   protected class PopulateOrderThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private int id_warehouse;
      private int id_district;

      @Override
      public String toString() {
         return "PopulateOrderThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", id_warehouse=" + id_warehouse +
               ", id_district=" + id_district +
               '}';
      }

      public PopulateOrderThread(long l, long u, int w, int d){
         this.lowerBound = l;
         this.upperBound = u;
         this.id_district = d;
         this.id_warehouse = w;
      }

      public void run(){
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder) / elementsPerBlock;
         long base = lowerBound;

         for(int batch = 1; batch <= numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            executeTransaction(base, base + elementsPerBlock);
            base += elementsPerBlock;
         }

         logRemainder(toString());
         executeTransaction(base, upperBound + 1);

         logFinish(toString());
      }

      private void executeTransaction(long start, long end) {
         logOrderPopulation(id_warehouse, id_district, start, end - 1);
         LinkedList<Integer> seqAleaList = new LinkedList<Integer>();
         boolean useList = false;

         do {
            startTransactionIfNeeded();
            Iterator<Integer> iterator = seqAleaList.iterator();

            for(long id_order=start; id_order < end; id_order++){

               int generatedSeqAlea;

               if (useList && iterator.hasNext()) {
                  generatedSeqAlea = iterator.next();
               } else {
                  generatedSeqAlea = generateSeqAlea(0, TpccTools.NB_MAX_CUSTOMER-1);
                  seqAleaList.add(generatedSeqAlea);
               }

               int o_ol_cnt = TpccTools.aleaNumber(5, 15);
               Date aDate = new Date((new java.util.Date()).getTime());

               Order newOrder= new Order(id_order,
                                         id_district,
                                         id_warehouse,
                                         generatedSeqAlea,
                                         aDate,
                                         (id_order < TpccTools.LIMIT_ORDER) ? TpccTools.aleaNumber(1, 10):0,
                                         o_ol_cnt,
                                         1);

               if (!txAwarePut(newOrder)) {
                  break; // rollback tx
               }
               populateOrderLines(id_warehouse, id_district, (int)id_order, o_ol_cnt, aDate);

               if (id_order >= TpccTools.LIMIT_ORDER){
                  populateNewOrder(id_warehouse, id_district, (int)id_order);
               }
            }
            useList = true;
         } while (!endTransactionIfNeeded());
      }
   }

   protected class PopulateCustomerThread extends Thread{
      private long lowerBound;
      private long upperBound;
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

      @SuppressWarnings("unchecked")
      public PopulateCustomerThread(long lowerBound, long upperBound, int id_warehouse, int id_district,
                                    ConcurrentHashMap c){
         this.lowerBound = lowerBound;
         this.upperBound = upperBound;
         this.id_district = id_district;
         this.id_warehouse = id_warehouse;
         this.lookupContentionAvoidance = c;
      }

      public void run(){
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder)  / elementsPerBlock;
         long base = lowerBound;

         for(int batch =1; batch <= numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            executeTransaction(base, base + elementsPerBlock);
            base += elementsPerBlock;
         }

         logRemainder(toString());
         executeTransaction(base, upperBound + 1);

         logFinish(toString());
      }

      private void executeTransaction(long start, long end) {
         logCustomerPopulation(id_warehouse, id_district, start, end - 1);
         do {
            startTransactionIfNeeded();
            for(long id_customer = start; id_customer < end; id_customer++) {
               String c_last = c_last();

               Customer newCustomer = new Customer(id_warehouse,
                                                   id_district,
                                                   id_customer,
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

               if (!txAwarePut(newCustomer)) {
                  break; // rollback tx
               }

               if(isBatchingEnabled()){
                  CustomerLookupQuadruple clt = new CustomerLookupQuadruple(c_last,id_warehouse,id_district, id_customer);
                  if(!this.lookupContentionAvoidance.containsKey(clt)){
                     this.lookupContentionAvoidance.put(clt,1);
                  }
               } else{
                  CustomerLookup customerLookup = new CustomerLookup(c_last, id_warehouse, id_district);
                  if (!txAwareLoad(customerLookup)) {
                     break; // rollback tx
                  }
                  customerLookup.addId(id_customer);

                  if (!txAwarePut(customerLookup)) {
                     break; // rollback tx
                  }
               }

               populateHistory((int)id_customer, id_warehouse, id_district);
            }
         } while (!endTransactionIfNeeded());
      }
   }

   protected class PopulateItemThread extends Thread{

      private long lowerBound;
      private long upperBound;

      @Override
      public String toString() {
         return "PopulateItemThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               '}';
      }

      public PopulateItemThread(long low, long up){
         this.lowerBound = low;
         this.upperBound = up;
      }

      public void run(){
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerBlock;
         long base = lowerBound;

         for(long batch = 1; batch <=numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            executeTransaction(base, base + elementsPerBlock);
            base += elementsPerBlock;
         }

         logRemainder(toString());
         executeTransaction(base, upperBound + 1);

         logFinish(toString());
      }

      private void executeTransaction(long start, long end) {
         logItemsPopulation(start, end - 1);
         do {
            startTransactionIfNeeded();
            for(long id_item = start; id_item < end; id_item++){
               Item newItem = new Item(id_item,
                                       TpccTools.aleaNumber(1, 10000),
                                       TpccTools.aleaChainec(14, 24),
                                       TpccTools.aleaFloat(1, 100, 2),
                                       TpccTools.sData());
               if (!txAwarePut(newItem)) {
                  break; //rollback tx;
               }
            }
         } while (!endTransactionIfNeeded());
      }
   }

   protected class PopulateStockThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private int id_warehouse;

      @Override
      public String toString() {
         return "PopulateStockThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", id_warehouse=" + id_warehouse +
               '}';
      }

      public PopulateStockThread(long low, long up, int id_warehouse){
         this.lowerBound = low;
         this.upperBound = up;
         this.id_warehouse = id_warehouse;
      }

      public void run(){
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerBlock;
         long base = lowerBound;

         for(long batch = 1; batch <=numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            executeTransaction(base, base + elementsPerBlock);
            base += elementsPerBlock;
         }

         logRemainder(toString());
         executeTransaction(base, upperBound + 1);

         logFinish(toString());
      }

      private void executeTransaction(long start, long end) {
         logStockPopulation(id_warehouse, start, end - 1);
         do {
            startTransactionIfNeeded();
            for(long id_stock = start; id_stock < end; id_stock++){
               Stock newStock=new Stock(id_stock,
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
               if (!txAwarePut(newStock)) {
                  break;
               }
            }
         } while (!endTransactionIfNeeded());
      }
   }

   protected class PopulateCustomerLookupThread extends Thread{
      private Vector<CustomerLookupQuadruple> vector;
      private long lowerBound;
      private long upperBound;

      @Override
      public String toString() {
         return "PopulateCustomerLookupThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               '}';
      }

      @SuppressWarnings("unchecked")
      public PopulateCustomerLookupThread(long l, long u, Vector v){
         this.vector = v;
         this.lowerBound = l;
         this.upperBound = u;
      }

      public void run(){
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerBlock;
         long base = lowerBound;

         for(long batch = 1; batch <= numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            executeTransaction(base, base + elementsPerBlock);
            base += elementsPerBlock;
         }

         logRemainder(toString());
         executeTransaction(base, upperBound + 1);

         logFinish(toString());
      }

      private void executeTransaction(long start, long end) {
         logCustomerLookupPopulation(start, end - 1);
         do {
            startTransactionIfNeeded();
            for(long idx = start; idx < end; idx++){

               CustomerLookupQuadruple clq = this.vector.get((int)idx);
               CustomerLookup customerLookup = new CustomerLookup(clq.c_last, clq.id_warehouse, clq.id_district);

               if (!txAwareLoad(customerLookup)) {
                  break; //rollback tx
               }

               customerLookup.addId(clq.id_customer);

               if (!txAwarePut(customerLookup)) {
                  break; //rollback tx
               }
            }
         } while (!endTransactionIfNeeded());
      }
   }

   protected class CustomerLookupQuadruple {
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
         return id_district == that.id_district &&
               id_warehouse == that.id_warehouse &&
               !(c_last != null ? !c_last.equals(that.c_last) : that.c_last != null);

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

   protected final boolean isBatchingEnabled(){
      return this.elementsPerBlock != 1;
   }

   private void startTransactionIfNeeded() {
      if (isBatchingEnabled()) {
         //Pedro: this is experimental. I want to avoid the overloading of the network. 
         // So, instead of starting immediately the transaction, it waits a while
         long sleepFor = waitingPeriod.get();

         if (sleepFor > 0) {
            sleepFor(sleepFor);
         }
         wrapper.startTransaction();
      }
   }

   private boolean endTransactionIfNeeded() {
      if (!isBatchingEnabled()) {
         return true;
      }

      long start = System.currentTimeMillis();
      try {
         wrapper.endTransaction(true);
      } catch (Throwable t) {
         log.warn("Error committing transaction. Error is " + t.getMessage(), t);
         try {
            wrapper.endTransaction(false);
         } catch (Throwable t2) {
            //just ignore
         }
         sleepRandomly();
         log.warn("Retrying transaction...");
         return false;
      } finally {
         calculateNextWaitingTime(System.currentTimeMillis() - start);
      }
      return true;
   }

   private void calculateNextWaitingTime(long duration) {
      if (duration <= 10) {
         long old = waitingPeriod.get();
         waitingPeriod.set(old / 2);
         return ;
      }
      int counter = 0;
      while (duration > 0) {
         counter++;
         duration /= 10;
      }
      waitingPeriod.addAndGet(counter);
   }

   private void sleepRandomly() {
      Random r = new Random();
      long sleepFor;
      do {
         sleepFor = r.nextLong();
      } while (sleepFor <= 0);
      sleepFor(sleepFor % MAX_SLEEP_BEFORE_RETRY);
   }

   private void sleepFor(long milliseconds) {
      try {
         Thread.sleep(milliseconds);
      } catch (InterruptedException e) {
         //no-op
      }
   }

   private void logStart(String thread) {
      log.debug("Starting " + thread);
   }

   private void logFinish(String thread) {
      log.debug("Ended " + thread);
   }

   private void logBatch(String thread, long batch, long numberOfBatches) {
      log.debug(thread + " is populating the " + batch + " batch out of " + numberOfBatches);
   }

   private void logRemainder(String thread) {
      log.debug(thread + " is populating the remainder");
   }

   private void logCustomerLookupPopulation(long init, long end) {
      log.debug("Populate Customer Lookup from index " + init + " to " + end);
   }

   protected void performMultiThreadPopulation(long initValue, long numberOfItems, ThreadCreator threadCreator) {
      Thread[] threads = new Thread[parallelThreads];

      //compute the number of item per thread
      long threadRemainder = numberOfItems % parallelThreads;
      long itemsPerThread = (numberOfItems - threadRemainder) / parallelThreads;

      long lowerBound = initValue;
      long itemsToAdd;

      for(int i = 1; i <= parallelThreads; i++){
         itemsToAdd = itemsPerThread + (i == parallelThreads ? threadRemainder:0);
         Thread thread = threadCreator.createThread(lowerBound, lowerBound + itemsToAdd - 1);
         threads[i-1] = thread;
         thread.start();
         lowerBound += (itemsToAdd);
      }

      //wait until all thread are finished
      try{
         for(Thread thread : threads){
            log.trace("Waiting for the end of " + thread);
            thread.join();
         }
         log.trace("All threads have finished! Movin' on");
      }
      catch(InterruptedException ie){
         ie.printStackTrace();
         System.exit(-1);
      }
   }

   protected interface ThreadCreator {
      Thread createThread(long lowerBound, long upperBound);
   }
}
