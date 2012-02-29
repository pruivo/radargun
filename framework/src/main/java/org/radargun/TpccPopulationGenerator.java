package org.radargun;

import org.radargun.tpcc.SerialTpccPopulationGenerator;
import org.radargun.utils.TypedProperties;
import org.radargun.utils.Utils;

import java.io.FileReader;
import java.util.Properties;

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
        int olIdMast = Integer.parseInt(properties.getProperty("tpcc.olIdMask"));
        int cIdMask = Integer.parseInt(properties.getProperty("tpcc.cIdMask"));

        String plugin = Utils.getCacheWrapperFqnClass(productName);
        CacheWrapper wrapper = (CacheWrapper) Class.forName(plugin).newInstance();
        wrapper.setUp(config, true, 0, new TypedProperties());

        SerialTpccPopulationGenerator serialTpccPopulationGenerator = new SerialTpccPopulationGenerator(wrapper,
                numOfWarehouses, cLastMask, olIdMast, cIdMask, maxNumberOfSlaves);

        long start = System.nanoTime();
        serialTpccPopulationGenerator.populate();
        long duration = System.nanoTime() - start;

        System.out.println("Population duration " + duration + " nanoseconds");
    }
}
