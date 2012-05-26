package org.radargun.fwk;

import org.radargun.keygenerator.KeyGenerator.KeyGeneratorFactory;
import org.radargun.keygenerator.WarmupIterator;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Simple test to key generator
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
@Test
public class KeyGeneratorTest {

   public void testSingleMasterShared() {
      int numberOfNodes = 10;
      int numberOfThreads = 8;
      int numberOfKeys = 1000;

      KeyGeneratorFactory factory = new KeyGeneratorFactory(numberOfNodes, numberOfThreads, numberOfKeys, 1000, true, null);
      List<WarmupIterator> list = factory.constructForWarmup(false, true);      
      assertIteratorListSize(list, 0);

      list = factory.constructForWarmup(true, true);
      assertIteratorListSize(list, 1);
      
      assertShared(list.get(0), numberOfKeys);   
   }

   public void testSingleMasterPrivate() {
      int numberOfNodes = 10;
      int numberOfThreads = 8;
      int numberOfKeys = 1000;      

      KeyGeneratorFactory factory = new KeyGeneratorFactory(numberOfNodes, numberOfThreads, numberOfKeys, 1000, false, null);
      List<WarmupIterator> list = factory.constructForWarmup(true, true);
      assertIteratorListSize(list, (numberOfNodes * numberOfThreads) + 1);      
      
      int it = 0;
      for (int nodeIdx = 0; nodeIdx < numberOfNodes; ++nodeIdx) {
         for (int threadIdx = 0; threadIdx < numberOfThreads; ++threadIdx) {
            assertPrivate(list.get(it++), 12, nodeIdx, threadIdx);
         }
      }
      assertShared(list.get(it), 40);

      list = factory.constructForWarmup(false, true);
      assertIteratorListSize(list, 0);      
   }

   public void testMultiMasterShared() {
      int numberOfNodes = 2;
      int numberOfThreads = 8;
      int numberOfKeys = 1000;

      KeyGeneratorFactory factory = new KeyGeneratorFactory(numberOfNodes, numberOfThreads, numberOfKeys, 1000, true, null);
      List<WarmupIterator> list = factory.constructForWarmup(0, true);
      assertIteratorListSize(list, 1);
      
      assertShared(list.get(0), 0, 499);

      list = factory.constructForWarmup(1, true);
      assertIteratorListSize(list, 1);
      
      assertShared(list.get(0), 500, 999);     
   }

   public void testMultiMasterPrivate() {
      int numberOfNodes = 2;
      int numberOfThreads = 8;
      int numberOfKeys = 1000;

      KeyGeneratorFactory factory = new KeyGeneratorFactory(numberOfNodes, numberOfThreads, numberOfKeys, 1000, false, null); 
      List<WarmupIterator> list = factory.constructForWarmup(0, true);             
      assertIteratorListSize(list, numberOfThreads + 1);
      
      int it = 0;
      for (int threadIdx = 0; threadIdx < numberOfThreads; ++threadIdx) {
         assertPrivate(list.get(it++), 62, 0, threadIdx);
      }
      assertShared(list.get(it), 8);

      list = factory.constructForWarmup(1, true);     
      assertIteratorListSize(list, numberOfThreads);
      
      it = 0;
      for (int threadIdx = 0; threadIdx < numberOfThreads; ++threadIdx) {
         assertPrivate(list.get(it++), 62, 1, threadIdx);
      }
   }
   
   public void testIterator() {
      KeyGeneratorFactory factory = new KeyGeneratorFactory(1, 1, 5, 1000, true, null);
      List<WarmupIterator> list = factory.constructForWarmup(0, true);            
      assertIteratorListSize(list, 1);
      
      WarmupIterator iterator = list.get(0);
      iterator.setCheckpoint();      
      assertShared(iterator, 5);      
      iterator.returnToLastCheckpoint();
      assertShared(iterator, 5);
      iterator.returnToLastCheckpoint();
      assertShared(iterator, 5);
   }
   
   public void testCheckUpdate() {      
      KeyGeneratorFactory factory = new KeyGeneratorFactory(1, 1, 5, 1000, true, null);
      System.out.println("Should print warnings");
      factory.checkAndUpdate(2, 2, 10, 2000, false, "bucket");
      System.out.println("Should *not* print warnings");
      factory.checkAndUpdate(2, 2, 10, 2000, false, "bucket");
   }   

   private void assertShared(WarmupIterator warmupIterator, int numberOfKeys) {
      assertShared(warmupIterator, 0, numberOfKeys);
   }

   private void assertShared(WarmupIterator warmupIterator, int start, int end) {
      for (int i = start; i < end; ++i) {
         assert warmupIterator.hasNext() : "Expected next element";
         assert warmupIterator.next().equals("KEY_" + i) : "Different key expected";
      }
   }

   private void assertPrivate(WarmupIterator warmupIterator, int numberOfKeys, int nodeIdx, int threadIdx) {
      for (int i = 0; i < numberOfKeys; ++i) {
         assert warmupIterator.hasNext() : "Expected next element";
         assert warmupIterator.next().equals("KEY_" + nodeIdx + "_" + threadIdx + "_" + i) : "Different key expected";
      }
   }
   
   private void assertIteratorListSize(List<?> list, int size) {
      assert list.size() == size : "Expected " + size + " iterator(s) in list but it has " + list.size();
   }

}
