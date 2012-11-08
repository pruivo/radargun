package org.radargun.fwk;

import org.radargun.tpcc.ThreadParallelTpccPopulation;
import org.radargun.tpcc.TpccTools;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.String.format;
import static org.radargun.tpcc.ThreadParallelTpccPopulation.performMultiThreadPopulation;
import static org.radargun.tpcc.TpccPopulation.SplitIndex;
import static org.radargun.tpcc.TpccPopulation.split;

/**
 * Check if the split used in population is working properly
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
@Test
public class SplitPopulationTest {

   private static final int NUMBER_OF_THREAD = 2;
   private final ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_THREAD);

   public void testSingleCase() {
      populate(10, 2, 1, 50);
   }

   public void test1() {
      for (int numberOfItems = 10; numberOfItems < 1000000; numberOfItems *= 10) {
         for (int numberOfNodes = 1; numberOfNodes < 10; ++numberOfNodes) {
            for (int numberOfThreads = 1; numberOfThreads < 100; numberOfThreads *= 2) {
               populate(numberOfItems, numberOfNodes, numberOfThreads, 50);
            }
         }
         for (int numberOfNodes = 10; numberOfNodes < 100; numberOfNodes += 10) {
            for (int numberOfThreads = 1; numberOfThreads < 100; numberOfThreads *= 2) {
               populate(numberOfItems, numberOfNodes, numberOfThreads, 50);
            }
         }
      }
   }

   public void testDistrict() {
      populateRealCase(TpccTools.NB_MAX_DISTRICT);
   }

   public void testItem() {
      populateRealCase(TpccTools.NB_MAX_ITEM);
   }

   public void testCustomer() {
      populateRealCase(TpccTools.NB_MAX_CUSTOMER);
   }

   public void testOrder() {
      populateRealCase(TpccTools.NB_MAX_ORDER);
   }

   private void populateRealCase(int numberOfItems) {
      for (int numberOfNodes = 1; numberOfNodes < 10; ++numberOfNodes) {
         for (int numberOfThreads = 1; numberOfThreads < 100; numberOfThreads *= 2) {
            for (int batch = 1; batch < 200; batch *= 2) {
               populate(numberOfItems, numberOfNodes, numberOfThreads, batch *= 2);
            }
         }
      }
      for (int numberOfNodes = 10; numberOfNodes < 100; numberOfNodes += 10) {
         for (int numberOfThreads = 1; numberOfThreads < 100; numberOfThreads *= 2) {
            for (int batch = 1; batch < 200; batch *= 2) {
               populate(numberOfItems, numberOfNodes, numberOfThreads, batch *= 2);
            }
         }
      }
   }

   private void populate(final int numberOfItems, final int numberOfNodes, int numberOfThreads, final int batch) {
      final Set<Integer> population = Collections.synchronizedSet(new TreeSet<Integer>());
      final int[] elementsPerNode = new int[numberOfNodes];
      Arrays.fill(elementsPerNode, 0);
      for (int nodeIdx = 0; nodeIdx < numberOfNodes; ++nodeIdx) {
         final SplitIndex splitIndex = split(numberOfItems, numberOfNodes, nodeIdx);
         performMultiThreadPopulation(splitIndex.getStart(), splitIndex.getEnd(), numberOfThreads, new ThreadParallelTpccPopulation.ThreadCreator() {
            @Override
            public Callable<Object> createThread(long lowerBound, long upperBound) {
               return new Populate(lowerBound, upperBound, population, numberOfNodes, numberOfItems, batch);
            }
         }, executorService);
      }
      assertPopulation(population, numberOfItems, numberOfNodes, numberOfThreads, batch);
      System.out.println(format("[%s,%s,%s,%s] done!", numberOfItems, numberOfNodes, numberOfThreads, batch));
   }

   private void assertPopulation(Set<Integer> integers, int numberOfItems, int numberOfNodes, int numberOfThreads, int batch) {
      assert integers.size() == numberOfItems : format("[%s,%s] number of items is different %s != %s", numberOfItems,
                                                       numberOfNodes, integers.size(), numberOfItems);
      for (int i = 1; i <= numberOfItems; ++i) {
         assert integers.contains(i) : format("[%s,%s,%s,%s] population does not have index %s", numberOfItems,
                                              numberOfNodes, numberOfThreads, batch, i);
      }
   }

   private class Populate extends ThreadParallelTpccPopulation.PopulationThread {

      private final Set<Integer> populate;
      private final int numberOfNodes;
      private final int numberOfItems;

      protected Populate(long lowerBound, long upperBound, Set<Integer> populate, int numberOfNodes, int numberOfItems,
                         int batch) {
         super(lowerBound, upperBound, batch);
         this.populate = populate;
         this.numberOfNodes = numberOfNodes;
         this.numberOfItems = numberOfItems;
      }

      @Override
      protected void executeTransaction(long start, long end) {
         for (long i = start; i <= end; ++i) {
            if (!populate.add((int)i)) {
               assert false : format("[%s,%s] duplicated index %s", numberOfItems, numberOfNodes, i);
            }
         }
      }

      @Override
      public String toString() {
         return "Populate{" +
               "numberOfNodes=" + numberOfNodes +
               ", numberOfItems=" + numberOfItems +
               ", " + super.toString();
      }
   }
}
