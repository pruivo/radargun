package org.radargun.keygenerator;

/**
 * A shared key generator by all threads and nodes/slaves
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class SharedKeyGenerator extends AbstractKeyGenerator {

   public SharedKeyGenerator(int threadIdx, int numberOfKeys, int valueSize, String bucketPrefix) {
      super(-1, threadIdx, 0, numberOfKeys, valueSize, bucketPrefix);
   }

   SharedKeyGenerator(int threadIdx, int startIdx, int endIdx, int valueSize, String bucketPrefix) {
      super(-1, threadIdx, startIdx, endIdx, valueSize, bucketPrefix);
   }

   @Override
   protected final String getKey(int idx) {
      return "KEY_" + idx;
   }
}
