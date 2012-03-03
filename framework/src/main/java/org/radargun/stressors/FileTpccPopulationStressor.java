package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stages.TpccPopulationStage;
import org.radargun.tpcc.FileTpccPopulation;
import org.radargun.tpcc.TpccTools;

import java.util.Map;

/**
 * Populate <code>numWarehouses</code> Warehouses in cache.
 *
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 */
public class FileTpccPopulationStressor extends AbstractCacheWrapperStressor {

   private static Log log = LogFactory.getLog(TpccPopulationStage.class);

   private int numWarehouses;

   private long cLastMask = 255L;

   private long olIdMask = 8191L;

   private long cIdMask = 1023L;

   private int numSlaves;

   private String folder;

   private CacheWrapper wrapper;

   public Map<String, String> stress(CacheWrapper wrapper) {
      if (wrapper == null) {
         throw new IllegalStateException("Null wrapper not allowed");
      }
      this.wrapper = wrapper;
      try {
         log.info("Performing Population Operations");
         performPopulationOperations();
      } catch (Exception e) {
         log.warn("Received exception during cache population" + e.getMessage());
      }
      return null;
   }

   private void performPopulationOperations() throws Exception {
      FileTpccPopulation population = new FileTpccPopulation(numWarehouses, cLastMask, olIdMask, cIdMask, folder);
      log.info("Performing population...");
      population.populateFromFile(numSlaves, wrapper);
      log.info("Population ended");
   }

   public void setNumWarehouses(int numWarehouses) {
      this.numWarehouses = numWarehouses;
   }

   public void setNumSlaves(int numSlaves) {
      this.numSlaves = numSlaves;
   }

   public void setCLastMask(long cLastMask) {
      this.cLastMask = cLastMask;
   }

   public void setOlIdMask(long olIdMask) {
      this.olIdMask = olIdMask;
   }

   public void setCIdMask(long cIdMask) {
      this.cIdMask = cIdMask;
   }

   public void setFolder(String folder) {
      this.folder = folder;
   }

   @Override
   public String toString() {
      return "TpccPopulationStressor{" +
            "numWarehouses=" + this.numWarehouses +
            "cLastMask=" + TpccTools.A_C_LAST +
            "olIdMask=" + TpccTools.A_OL_I_ID +
            "cIdMask=" + TpccTools.A_C_ID +
            "folder=" + this.folder +
            "numSlaves=" + this.numSlaves + "}";
   }


   public void destroy() throws Exception {
      //Don't destroy data in cache!
   }

}
