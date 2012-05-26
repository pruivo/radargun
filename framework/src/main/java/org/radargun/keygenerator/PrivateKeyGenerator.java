package org.radargun.keygenerator;

/**
 * A private key generator in order to avoid any contention between threads and nodes/slaves 
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class PrivateKeyGenerator extends AbstractKeyGenerator {

   public PrivateKeyGenerator(int slaveIdx, int threadIdx, int numberOfKeys, int valueSize, String bucketPrefix) {
      super(slaveIdx, threadIdx, 0, numberOfKeys, valueSize, bucketPrefix);
   }

   @Override
   protected final String getKey(int idx) {
      return "KEY_" + slaveIdx + "_" + threadIdx + "_" + idx;
   }
}
