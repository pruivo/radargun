package org.radargun.keygen2;

import java.io.Serializable;

/**
 * Key type for synthetic radargun tests
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class RadargunKey implements Serializable {

    private static final transient String KEY_FORMAT = "KEY_%s_%s_%s";

    private final int nodeIdx;
    private final int threadIdx;
    private final int keyIdx;

    public RadargunKey(int nodeIdx, int threadIdx, int keyIdx) {
        this.nodeIdx = nodeIdx;
        this.threadIdx = threadIdx;
        this.keyIdx = keyIdx;
    }

    public int getNodeIdx() {
        return nodeIdx;
    }

    public int getThreadIdx() {
        return threadIdx;
    }

    public int getKeyIdx() {
        return keyIdx;
    }

    @Override
    public String toString() {
        return String.format(KEY_FORMAT, nodeIdx, threadIdx, keyIdx);
    }
}
