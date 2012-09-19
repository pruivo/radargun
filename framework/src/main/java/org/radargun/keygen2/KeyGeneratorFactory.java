package org.radargun.keygen2;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * New Key Generator Factory that builds keys with the node index, thread index and a key index. 
 * Useful for the data placement algorithm
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class KeyGeneratorFactory {

   public static final String SEPARATOR = "_";

   private final Object calculateLock = new Object();

   private int numberOfNodes;
   private int numberOfThreads;
   private int numberOfKeys;
   private int valueSize;
   private boolean noContention;
   private final String keyPrefix;
   private String bucketPrefix;

   private final AtomicReference<Workload> currentWorkload;

   public KeyGeneratorFactory() {
      this(1000, 1, 1, 1000, false, "KEY", "BUCKET");
   }

   private KeyGeneratorFactory(int numberOfKeys, int numberOfThreads, int numberOfNodes, int valueSize, boolean noContention,
                               String keyPrefix, String bucketPrefix) {
      this.keyPrefix = keyPrefix.replaceAll(SEPARATOR, "");
      currentWorkload = new AtomicReference<Workload>();
      setBucketPrefix(bucketPrefix);
      setNumberOfKeys(numberOfKeys);
      setNumberOfNodes(numberOfNodes);
      setNumberOfThreads(numberOfThreads);
      setNoContention(noContention);
      setValueSize(valueSize);
   }

   public void setValueSize(int valueSize) {
      this.valueSize = valueSize;
   }

   public void calculate() {
      synchronized (calculateLock) {
         int nodeIdx = numberOfKeys % numberOfNodes;
         int numberOfKeysPerNode = (numberOfKeys - nodeIdx) / numberOfNodes;
         int threadIdx = numberOfKeysPerNode % numberOfThreads;
         int numberOfKeysPerThread = (numberOfKeysPerNode - threadIdx) / numberOfThreads;
         currentWorkload.set(new Workload(numberOfNodes, numberOfThreads, numberOfKeysPerThread, nodeIdx, threadIdx,
                                          noContention));
      }
   }

   public void setNumberOfNodes(int numberOfNodes) {
      if (numberOfNodes > 0) {
         this.numberOfNodes = numberOfNodes;
      }
   }

   public void setBucketPrefix(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix == null ? "BUCKET" : bucketPrefix.replaceAll(SEPARATOR, "");
   }

   public void setNumberOfThreads(int numberOfThreads) {
      if (numberOfThreads > 0) {
         this.numberOfThreads = numberOfThreads;
      }
   }

   public void setNumberOfKeys(int numberOfKeys) {
      if (numberOfKeys > 0) {
         this.numberOfKeys = numberOfKeys;
      }
   }

   public void setNoContention(boolean noContention) {
      this.noContention = noContention;
   }

   public KeyGenerator createKeyGenerator(int nodeIdx, int threadIdx) {
      return new InternalKeyGenerator(nodeIdx, threadIdx);
   }

   public Iterator<WarmupEntry> warmup(int nodeIdx) {
      return new InternalNodeIterator(nodeIdx, currentWorkload.get());
   }

   public Iterator<WarmupEntry> warmupAll() {
      return new InternalAllNodesIterator(currentWorkload.get());
   }

   public int getNumberOfThreads() {
      return numberOfThreads;
   }

   public boolean isNoContention() {
      return noContention;
   }

   public int getValueSize() {
      return valueSize;
   }

   public int getNumberOfKeys() {
      return numberOfKeys;
   }

   @Override
   public String toString() {
      return "KeyGeneratorFactory{" +
            "numberOfNodes=" + numberOfNodes +
            ", numberOfThreads=" + numberOfThreads +
            ", numberOfKeys=" + numberOfKeys +
            ", valueSize=" + valueSize +
            ", noContention=" + noContention +
            ", keyPrefix='" + keyPrefix + '\'' +
            ", bucketPrefix='" + bucketPrefix + '\'' +
            '}';
   }

   private Object getRandomValue(Random random) {
      // each char is 2 bytes
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < valueSize / 2; i++) {
         sb.append((char) (64 + random.nextInt(26)));
      }
      return sb.toString();
   }

   private Object createKey(int nodeIdx, int threadIdx, int keyIdx) {
      return keyPrefix + SEPARATOR + nodeIdx + SEPARATOR + threadIdx + SEPARATOR + keyIdx;
   }

   private int maxKeyIdx(Workload workload, int nodeIdx, int threadIdx) {
      return workload.keyPerThread + (nodeIdx < workload.nodeIdx ? 1 : 0) + (threadIdx < workload.threadIdx ? 1 : 0);
   }

   private String getBucket(int threadIdx) {
      return bucketPrefix + SEPARATOR + threadIdx;
   }

   private class InternalNodeIterator implements Iterator<WarmupEntry> {

      protected int nodeIdx;
      protected final Workload workload;
      protected int threadIdx = 0;
      protected int keyIdx = 0;
      protected int maxKeyIdx = 0;
      private final Random random;

      private InternalNodeIterator(int nodeIdx, Workload workload) {
         random = new Random(System.currentTimeMillis() >> nodeIdx);
         this.nodeIdx = nodeIdx;
         this.workload = workload;
         updateMaxKeyIndex();
      }

      @Override
      public boolean hasNext() {
         return nodeIdx < workload.numberOfNodes && threadIdx < workload.numberOfThreads && keyIdx < maxKeyIdx;
      }

      @Override
      public WarmupEntry next() {
         if (!hasNext()) {
            throw new NoSuchElementException();
         }
         WarmupEntry next = new WarmupEntry(getBucket(threadIdx),
                                            createKey(nodeIdx, threadIdx, keyIdx),
                                            getRandomValue(random));
         increment();
         return next;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      protected void updateMaxKeyIndex() {
         maxKeyIdx = maxKeyIdx(workload, nodeIdx, threadIdx);
      }

      protected void increment() {
         keyIdx = (keyIdx + 1) % maxKeyIdx;
         if (keyIdx == 0) {
            threadIdx++;
            updateMaxKeyIndex();
         }
      }
   }

   private class InternalAllNodesIterator extends InternalNodeIterator {

      private InternalAllNodesIterator(Workload workload) {
         super(0, workload);
      }

      @Override
      protected void increment() {
         keyIdx = (keyIdx + 1) % maxKeyIdx;
         if (keyIdx == 0) {
            threadIdx = (threadIdx + 1) % workload.numberOfThreads;
            if (threadIdx == 0) {
               nodeIdx++;
            }
            updateMaxKeyIndex();
         }
      }
   }

   private class InternalKeyGenerator implements KeyGenerator {

      private final int nodeIdx;
      private final int threadIdx;
      private final Random random;

      private InternalKeyGenerator(int nodeIdx, int threadIdx) {
         this.nodeIdx = nodeIdx;
         this.threadIdx = threadIdx;
         random = new Random(System.currentTimeMillis() ^ nodeIdx << threadIdx);
      }

      @Override
      public Object getRandomKey() {
         Workload workload = currentWorkload.get();
         if (workload.noContention) {
            int keyIdx = random.nextInt(maxKeyIdx(workload, nodeIdx, threadIdx));
            return createKey(nodeIdx, threadIdx, keyIdx);
         } else {
            int nodeIdx = random.nextInt(workload.numberOfNodes);
            int threadIdx = random.nextInt(workload.numberOfThreads);
            int keyIdx = random.nextInt(maxKeyIdx(workload, nodeIdx, threadIdx));
            return createKey(nodeIdx, threadIdx, keyIdx);
         }
      }

      @Override
      public Object[] getUniqueRandomKeys(int size) {
         Set<Object> keys = new HashSet<Object>();
         Workload workload = currentWorkload.get();
         if (workload.noContention) {
            for (int i = 0; i < size; ++i) {
               int keyIdx = random.nextInt(maxKeyIdx(workload, nodeIdx, threadIdx));

               if (!findNoContentedNextUnique(keys, workload, keyIdx)) {
                  break;
               }
            }
         } else {
            for (int i = 0; i < size; ++i) {
               int nodeIdx = random.nextInt(workload.numberOfNodes);
               int threadIdx = random.nextInt(workload.numberOfThreads);
               int keyIdx = random.nextInt(maxKeyIdx(workload, nodeIdx, threadIdx));               
               
               if (!findNextUnique(keys, workload, nodeIdx, threadIdx, keyIdx)) {
                  break;
               }
            }
         }
         return keys.toArray();
      }

      @Override
      public String getBucket() {
         return KeyGeneratorFactory.this.getBucket(threadIdx);
      }

      @Override
      public Object getRandomValue() {
         return KeyGeneratorFactory.this.getRandomValue(random);
      }

      private boolean findNextUnique(Set<Object> alreadyCollected, Workload workload, int initialNodeIdx, int initialThreadIdx,
                                     int initialKeyIndex) {
         int nodeIdx = initialNodeIdx;
         int threadIdx = initialThreadIdx;
         int maxKeyIdx = maxKeyIdx(workload, nodeIdx, threadIdx);
         int keyIdx = initialKeyIndex;

         do {
            if (alreadyCollected.add(createKey(nodeIdx, threadIdx, keyIdx))) {
               return true;
            }

            keyIdx = (keyIdx + 1) % maxKeyIdx;
            if (keyIdx == 0) {
               threadIdx = (threadIdx + 1) % workload.numberOfThreads;
               if (threadIdx == 0) {
                  nodeIdx = (nodeIdx + 1) % workload.numberOfNodes;
               }
               maxKeyIdx = maxKeyIdx(workload, nodeIdx, threadIdx);
            }
         } while (nodeIdx != initialNodeIdx || threadIdx != initialThreadIdx || keyIdx != initialKeyIndex);

         return false;
      }

      private boolean findNoContentedNextUnique(Set<Object> alreadyCollected, Workload workload, int initialKeyIndex) {
         int maxKeyIdx = maxKeyIdx(workload, nodeIdx, threadIdx);
         int keyIdx = initialKeyIndex;

         do {
            if (alreadyCollected.add(createKey(nodeIdx, threadIdx, keyIdx))) {
               return true;
            }

            keyIdx = (keyIdx + 1) % maxKeyIdx;
         } while (keyIdx != initialKeyIndex);

         return false;
      }

   }

   private class Workload {
      private final int numberOfNodes;
      private final int numberOfThreads;
      private final int keyPerThread;
      private final int nodeIdx;
      private final int threadIdx;
      private final boolean noContention;

      private Workload(int numberOfNodes, int numberOfThreads, int keyPerThread, int nodeIdx, int threadIdx, boolean noContention) {
         this.numberOfNodes = numberOfNodes;
         this.numberOfThreads = numberOfThreads;
         this.keyPerThread = keyPerThread;
         this.nodeIdx = nodeIdx;
         this.threadIdx = threadIdx;
         this.noContention = noContention;
      }
   }
}
