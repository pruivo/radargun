package org.radargun.keygenerator;

import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

/**
 * The base implementation for a key generator with all the methods implemented
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public abstract class AbstractKeyGenerator implements KeyGenerator {
   private static final String DEFAULT_BUCKET_PREFIX = "BUCKET_";

   protected final int slaveIdx;
   protected final int threadIdx;
   private final int startIdx;
   private final int endIdx;
   private final int valueSize;
   private final String bucketPrefix;
   private final Random random;

   public AbstractKeyGenerator(int slaveIdx, int threadIdx, int startIdx, int endIdx, int valueSize, String bucketPrefix) {
      this.slaveIdx = slaveIdx;
      this.threadIdx = threadIdx;
      this.startIdx = startIdx;
      this.endIdx = endIdx;
      this.valueSize = valueSize;
      this.bucketPrefix = bucketPrefix == null ? DEFAULT_BUCKET_PREFIX : bucketPrefix;
      this.random = new Random(System.currentTimeMillis());
   }

   @Override
   public final String getRandomKey() {
      int idx = random.nextInt(endIdx - startIdx) + startIdx;
      return getKey(idx);
   }

   @Override
   public final String getRandomValue() {
      // each char is 2 bytes
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < valueSize / 2; i++) {
         sb.append((char) (64 + random.nextInt(26)));
      }
      return sb.toString();
   }

   @Override
   public final Collection<String> getAllKeys() {
      Set<String> result = new HashSet<String>(endIdx);
      for (int i = startIdx; i < endIdx; ++i) {
         result.add(getKey(i));
      }
      return result;
   }

   @Override
   public final WarmupIterator getWarmupIterator() {
      return new WarmupIteratorImpl(this);
   }

   @Override
   public final String getBucketPrefix() {
      return bucketPrefix + threadIdx;
   }

   @Override
   public final String toString() {
      return getClass().getSimpleName() +
            "{" +
            "slaveIdx=" + slaveIdx +
            ", threadIdx=" + threadIdx +
            ", startIdx=" + startIdx +
            ", endIdx=" + endIdx +
            ", valueSize=" + valueSize +
            '}';
   }

   /**
    * returns the key in the index idx. this is dependent if it generates a private key or a shared key
    *
    * @param idx  the index
    * @return     the key in the index idx
    */
   protected abstract String getKey(int idx);

   /**
    * the warmup iterator implementation
    */
   public static class WarmupIteratorImpl implements WarmupIterator {

      private final AbstractKeyGenerator keyGenerator;
      private int actualIdx;
      private int lastCheckpoint;

      public WarmupIteratorImpl(AbstractKeyGenerator keyGenerator) {
         this.keyGenerator = keyGenerator;
         this.actualIdx = keyGenerator.startIdx;
         this.lastCheckpoint = actualIdx;
      }

      @Override
      public final void setCheckpoint() {
         lastCheckpoint = actualIdx;
      }

      @Override
      public final void returnToLastCheckpoint() {
         actualIdx = lastCheckpoint;
      }

      @Override
      public final String getRandomValue() {
         return keyGenerator.getRandomValue();
      }

      @Override
      public final String getBucketPrefix() {
         return keyGenerator.getBucketPrefix();
      }

      @Override
      public final boolean hasNext() {
         return isInBounds();
      }

      @Override
      public final Object next() {
         if (!isInBounds()) {
            throw new NoSuchElementException();
         }
         return keyGenerator.getKey(actualIdx++);
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException("remote() is not supported in warmup iterator");
      }

      private boolean isInBounds() {
         return actualIdx >= keyGenerator.startIdx && actualIdx < keyGenerator.endIdx;
      }

      @Override
      public String toString() {
         return "WarmupIteratorImpl{" +
               "keyGenerator=" + keyGenerator +
               '}';
      }
   }
}
