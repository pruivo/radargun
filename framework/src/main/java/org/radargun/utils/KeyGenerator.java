package org.radargun.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author pruivo
 * @since 4.0
 */
public class KeyGenerator {

    private static final String DEFAULT_BUCKET_PREFIX = "BUCKET_";
    private static final String DEFAULT_KEY_PREFIX = "KEY_";

    private final int slaveIdx;
    private final int threadIdx;
    private final boolean noContention;
    private final String bucketPrefix;

    public KeyGenerator(int slaveIdx, int threadIdx, boolean noContention, String bucketPrefix) {
        this.slaveIdx = slaveIdx;
        this.threadIdx = threadIdx;
        this.noContention = noContention;
        this.bucketPrefix = bucketPrefix == null ? DEFAULT_BUCKET_PREFIX : bucketPrefix;
    }

    public String getKey(int keyIdx) {
        if(noContention) {
            return DEFAULT_KEY_PREFIX + slaveIdx + "_" + threadIdx + "_" + keyIdx;
        } else {
            return DEFAULT_KEY_PREFIX + keyIdx;
        }
    }

    public List<String> getAllKeys(int maxNumberOfKeys) {
        List<String> keys = new ArrayList<String>(maxNumberOfKeys);

        for (int i = 0; i < maxNumberOfKeys; ++i) {
            keys.add(getKey(i));
        }

        return keys;
    }

    public String getBucketPrefix() {
        return bucketPrefix + threadIdx;
    }
}
