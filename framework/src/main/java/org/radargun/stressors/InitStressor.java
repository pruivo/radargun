package org.radargun.stressors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.CacheWrapperStressor;
import org.radargun.utils.KeyGenerator;

import java.util.Map;
import java.util.Random;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class InitStressor implements CacheWrapperStressor {

    private static Log log = LogFactory.getLog(InitStressor.class);

    private Random r = new Random();

    private int slaveIdx = 0;

    //allows execution without contention
    private boolean noContentionEnabled = false;

    //for each there will be created fixed number of keys. All the GETs and PUTs are performed on these keys only.
    private int numberOfKeys = 1000;

    //Each key will be a byte[] of this size.
    private int sizeOfValue = 1000;

    //the number of threads that will work on this cache wrapper.
    private int numOfThreads = 1;

    private String bucketPrefix = null;


    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
        if (wrapper == null) {
            throw new IllegalStateException("Null wrapper not allowed");
        }

        if (noContentionEnabled) {
            noContentionWarmup(wrapper);
        } else if (slaveIdx == 0) {
            KeyGenerator keyGenerator = new KeyGenerator(slaveIdx, 0, false, bucketPrefix);
            warmup(wrapper, keyGenerator);
        }
        return null;
    }

    private void noContentionWarmup(CacheWrapper wrapper) {
        for (int threadIdx = 0; threadIdx < numOfThreads; ++threadIdx) {
            KeyGenerator keyGenerator = new KeyGenerator(slaveIdx, threadIdx, true, bucketPrefix);
            warmup(wrapper, keyGenerator);
        }
    }

    private void warmup(CacheWrapper cacheWrapper, KeyGenerator keyGenerator) {
        for (int i = 0; i < numberOfKeys; ++i) {
            try {
                cacheWrapper.put(keyGenerator.getBucketPrefix(), keyGenerator.getKey(i),
                        generateRandomString(sizeOfValue));
            } catch (Exception e) {
                log.warn("Exception occurred when put in " + keyGenerator.getKey(i) + ". " + e.getLocalizedMessage());
            }
        }
    }

    private String generateRandomString(int size) {
        // each char is 2 bytes
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size / 2; i++) sb.append((char) (64 + r.nextInt(26)));
        return sb.toString();
    }

    @Override
    public void destroy() throws Exception {
        //Do nothing... we don't want to loose the keys
    }

    public void setSlaveIdx(int slaveIdx) {
        this.slaveIdx = slaveIdx;
    }

    public void setNoContentionEnabled(boolean noContentionEnabled) {
        this.noContentionEnabled = noContentionEnabled;
    }

    public void setNumberOfKeys(int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
    }

    public void setSizeOfValue(int sizeOfValue) {
        this.sizeOfValue = sizeOfValue;
    }

    public void setNumOfThreads(int numOfThreads) {
        this.numOfThreads = numOfThreads;
    }

    public void setBucketPrefix(String bucketPrefix) {
        this.bucketPrefix = bucketPrefix;
    }
}
