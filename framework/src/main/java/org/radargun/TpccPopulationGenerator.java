package org.radargun;

import org.radargun.tpcc.FileTpccPopulation;

import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class TpccPopulationGenerator {

   public static void main(String[] args) throws Exception {
      Properties properties = new Properties();
      properties.load(new FileReader("conf/tpcc-gen.properties"));

      String productName = properties.getProperty("product.name");
      String config = properties.getProperty("product.config");
      int maxNumberOfSlaves = Integer.parseInt(properties.getProperty("product.maxNumberOfSlaves"));
      int numOfWarehouses = Integer.parseInt(properties.getProperty("tpcc.numWarehouses"));
      int cLastMask = Integer.parseInt(properties.getProperty("tpcc.cLastMask"));
      int olIdMask = Integer.parseInt(properties.getProperty("tpcc.olIdMask"));
      int cIdMask = Integer.parseInt(properties.getProperty("tpcc.cIdMask"));

      /*String plugin = Utils.getCacheWrapperFqnClass(productName);

 ClassLoader classLoader = Utils.buildProductSpecificClassLoader(productName, Thread.currentThread().getContextClassLoader());
 Thread.currentThread().setContextClassLoader(classLoader);

 CacheWrapper wrapper = (CacheWrapper) classLoader.loadClass(plugin).newInstance();
 TypedProperties typedProperties = new TypedProperties();
 typedProperties.put("tpcc.generator", true);
 wrapper.setUp(config, true, 0, typedProperties);

 SerialTpccPopulationGenerator serialTpccPopulationGenerator = new SerialTpccPopulationGenerator(wrapper,
         numOfWarehouses, cLastMask, olIdMast, cIdMask, maxNumberOfSlaves);

 long start = System.nanoTime();
 serialTpccPopulationGenerator.populate();
 long duration = System.nanoTime() - start;*/

      FileTpccPopulation fileTpccPopulation = new FileTpccPopulation(numOfWarehouses, cLastMask, olIdMask, cIdMask, "/Users/pruivo/datastore/");

      long start = System.nanoTime();
      long nrOfObjects = fileTpccPopulation.populateToFile();
      long duration = System.nanoTime() - start;
      System.out.println("Population duration " + TimeUnit.NANOSECONDS.toMillis(duration) + " milliseconds. " +
                               nrOfObjects + " objects saved");
   }
}
