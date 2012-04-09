package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.tpcc.domain.District;
import org.radargun.tpcc.domain.Warehouse;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class PassiveReplicationTpccPopulation extends ThreadParallelTpccPopulation {

   private static Log log = LogFactory.getLog(PassiveReplicationTpccPopulation.class);


   public PassiveReplicationTpccPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, int numSlaves,
                                           long cLastMask, long olIdMask, long cIdMask, int parallelThreads,
                                           int elementsPerBlock) {
      super(wrapper, numWarehouses, slaveIndex, numSlaves, cLastMask, olIdMask, cIdMask, parallelThreads, elementsPerBlock);
   }

   @Override
   public void performPopulation() {
      if (wrapper.canExecuteWriteTransactions()) {
         log.info("I am the primary and I am going to perform the population");
         super.performPopulation();
      } else {
         log.info("I am not allowed to perform the population.");
      }
   }

   @Override
   protected void populateWarehouses() {
      log.trace("Populate warehouses");

      for (int i = 1; i <= this.numWarehouses; i++) {
         log.info("Populate Warehouse " + i);

         Warehouse newWarehouse = new Warehouse(i,
                                                TpccTools.aleaChainec(6, 10),
                                                TpccTools.aleaChainec(10, 20), TpccTools.aleaChainec(10, 20),
                                                TpccTools.aleaChainec(10, 20), TpccTools.aleaChainel(2, 2),
                                                TpccTools.aleaChainen(4, 4) + TpccTools.CHAINE_5_1,
                                                TpccTools.aleaFloat(Float.valueOf("0.0000"), Float.valueOf("0.2000"), 4),
                                                TpccTools.WAREHOUSE_YTD);
         stubbornPut(newWarehouse);

         populateStock(i);

         populateDistricts(i);

         printMemoryInfo();
      }

   }

   @Override
   protected void populateDistricts(int id_warehouse) {
      if (id_warehouse < 0) {
         log.warn("Trying to populate Districts for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating District for warehouse " + id_warehouse);

      logDistrictPopulation(id_warehouse, 1, TpccTools.NB_MAX_DISTRICT);
      for (int id_district = 1; id_district <= TpccTools.NB_MAX_DISTRICT; id_district++) {
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
         stubbornPut(newDistrict);

         populateCustomers(id_warehouse, id_district);

         populateOrders(id_warehouse, id_district);
      }
   }

   @Override
   protected void populateItem() {
      log.trace("Populating Items");

      performMultiThreadPopulation(1, TpccTools.NB_MAX_ITEM, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateItemThread(lowerBound, upperBound);
         }
      });
   }

   @Override
   protected void populateStock(final int id_warehouse) {
      if (id_warehouse < 0) {
         log.warn("Trying to populate Stock for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating Stock for warehouse " + id_warehouse);

      performMultiThreadPopulation(1, TpccTools.NB_MAX_ITEM, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateStockThread(lowerBound, upperBound, id_warehouse);
         }
      });
   }
}
