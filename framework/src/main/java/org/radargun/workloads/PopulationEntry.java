package org.radargun.workloads;

/**
 * Represents an immutable cache entry with the key, bucket and value. It is used in the warmup iterators
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class PopulationEntry {

   private final String bucket;
   private final Object key;
   private final Object value;

   public PopulationEntry(String bucket, Object key, Object value) {
      this.bucket = bucket;
      this.key = key;
      this.value = value;
   }

   public String getBucket() {
      return bucket;
   }

   public Object getKey() {
      return key;
   }

   public Object getValue() {
      return value;
   }
}
