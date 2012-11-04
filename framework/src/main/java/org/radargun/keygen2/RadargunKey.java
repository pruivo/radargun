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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RadargunKey that = (RadargunKey) o;

      return keyIdx == that.keyIdx && nodeIdx == that.nodeIdx && threadIdx == that.threadIdx;

   }

   @Override
   public int hashCode() {
      int result = nodeIdx;
      result = 31 * result + threadIdx;
      result = 31 * result + keyIdx;
      return result;
   }
}
