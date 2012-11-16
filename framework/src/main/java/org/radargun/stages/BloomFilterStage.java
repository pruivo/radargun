package org.radargun.stages;

import com.elaunira.sbf.ScalableBloomFilter;
import com.elaunira.sbf.SlicedBloomFilter;
import edu.utexas.ece.mpc.bloomier.ImmutableBloomierFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.random.RandomAdaptor;
import org.infinispan.dataplacement.c50.lookup.BloomFilter;
import org.radargun.MasterStage;
import org.radargun.Stage;
import org.radargun.keygen2.RadargunKey;
import org.radargun.state.MasterState;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class BloomFilterStage implements MasterStage {

   private static final Log log = LogFactory.getLog(BloomFilterStage.class);
   private static final String GNUPLOT_SEPARATOR = "\t";
   private static final String GNUPLOT_COMMENT = "#";

   private String keysFilePath = null;
   private String roundsFilePath = null;
   private String outputFilePath = null;
   private double falsePositive;

   @Override
   public boolean execute() throws Exception {

      if (keysFilePath == null) {
         return logAndReturnError("Keys File Path not defined!");
      } else if (roundsFilePath == null) {
         return logAndReturnError("Keys Moved Format not defined!");
      } else if (outputFilePath == null) {
         return logAndReturnError("Output File Path not defined!");
      }

      try {
         log.info("Reading keys...");
         BufferedWriter writer = getBufferedWriterForOutput();
         List<RadargunKey> initialKeys = getKeys();
         log.info(initialKeys.size() + " keys read");

         int hashCodeConflict = 0;
         TreeSet<Integer> conflicts = new TreeSet<Integer>();
         Iterator<RadargunKey> it = initialKeys.iterator();
         while (it.hasNext()) {
            if (!conflicts.add(it.next().hashCode())) {
               it.remove();
               hashCodeConflict++;
            }
         }
         log.info("Hash code conflicts: " + hashCodeConflict + " out of " + initialKeys.size() + " keys");

         hashCodeConflict = 0;
         conflicts.clear();
         for (RadargunKey key : initialKeys) {
            if (!conflicts.add(key.hashCode())) {
               hashCodeConflict++;
            }
         }

         log.info("2nd Round Hash code conflicts: " + hashCodeConflict + " out of " + initialKeys.size() + " keys");

         Collection<Integer> keysPerRound = getKeysPerRound();
         log.info("Simulating " + keysPerRound + " rounds");

         Collections.shuffle(initialKeys);
         RadargunKey[] array = initialKeys.toArray(new RadargunKey[initialKeys.size()]);

         log.info("Writing header in file");
         writeHeader(writer);

         int roundId = 1;
         int iterator = 0;
         List<RadargunKey> keysMovedSoFar = new LinkedList<RadargunKey>();
         ScalableBloomFilter<RadargunKey> scalableBloomFilter = null;
         for (Integer totalKeysMoved : keysPerRound) {
            log.info("Start round " + roundId);
            List<RadargunKey> currentRound = new LinkedList<RadargunKey>();
            while (keysMovedSoFar.size() < totalKeysMoved && iterator < array.length) {
               keysMovedSoFar.add(array[iterator]);
               currentRound.add(array[iterator++]);
            }

            RoundStats stats = new RoundStats();
            stats.roundId = roundId;
            stats.totalKeys = array.length;
            stats.totalKeysMoved = totalKeysMoved;

            scalableBloomFilter = create(currentRound, scalableBloomFilter, stats);
            test(scalableBloomFilter, initialKeys, keysMovedSoFar, stats);
            stats.scalableBloomFilterSize = serializedSize(scalableBloomFilter);

            BloomFilter bloomFilter = create(keysMovedSoFar, stats);
            test(bloomFilter, initialKeys, keysMovedSoFar, stats);
            stats.bloomFilterSize = serializedSize(bloomFilter);

            ImmutableBloomierFilter<RadargunKey, Byte> bloomierFilter = createBloomierFilter(keysMovedSoFar, stats);
            if (bloomierFilter != null) {
               test(bloomierFilter, initialKeys, keysMovedSoFar, stats);
               stats.bloomierFilterSize = serializedSize(bloomierFilter);
            }

            write(writer, stats);
            log.info("Finish round " + roundId);
            roundId++;
         }

         writer.flush();
         writer.close();
      } catch (Exception e) {
         e.printStackTrace();
         return logAndReturnError("Exception caught: " + e.getMessage());
      }

      log.info("all done!");
      return true;
   }

   private ScalableBloomFilter<RadargunKey> create(List<RadargunKey> keyToAdd,
                                                   ScalableBloomFilter<RadargunKey> scalableBloomFilter,
                                                   RoundStats stats) {
      if (scalableBloomFilter == null) {
         long ts1 = System.nanoTime();
         scalableBloomFilter = new ScalableBloomFilter<RadargunKey>(keyToAdd.size(), falsePositive);
         for (RadargunKey key : keyToAdd) {
            scalableBloomFilter.add(key);
         }
         long ts2 = System.nanoTime();
         stats.scalableBloomFilterCreationTime = ts2 - ts1;
      } else {
         long ts1 = System.nanoTime();
         SlicedBloomFilter<RadargunKey> slicedBloomFilter = scalableBloomFilter.createNewSlice(keyToAdd.size());
         for (RadargunKey key : keyToAdd) {
            slicedBloomFilter.add(key);
         }
         scalableBloomFilter.add(slicedBloomFilter);
         long ts2 = System.nanoTime();
         stats.scalableBloomFilterCreationTime = ts2 - ts1;
         stats.incrementScalableBloomFilterSize = serializedSize(slicedBloomFilter);
      }

      return scalableBloomFilter;
   }

   private void test(ScalableBloomFilter<RadargunKey> scalableBloomFilter, List<RadargunKey> localKeys,
                     List<RadargunKey> keysMoved, RoundStats stats) {
      long duration = 0;
      int bfErrors = 0;
      for (RadargunKey key : localKeys) {
         long ts1 = System.nanoTime();
         boolean bfResult = scalableBloomFilter.contains(key);
         long ts2 = System.nanoTime();
         duration += ts2 - ts1;
         if (bfResult && !keysMoved.contains(key)) {
            bfErrors++;
         }
      }
      stats.scalableBloomFilterQueryTime = duration;
      stats.scalableBloomFilterErrors = bfErrors;
   }

   private BloomFilter create(List<RadargunKey> keyToAdd, RoundStats stats) {
      long ts1 = System.nanoTime();
      BloomFilter bloomFilter = new BloomFilter(falsePositive, keyToAdd.size());
      for (RadargunKey key : keyToAdd) {
         bloomFilter.add(key);
      }
      long ts2 = System.nanoTime();
      stats.bloomFilterCreationTime = ts2 - ts1;
      return bloomFilter;
   }

   private void test(BloomFilter bloomFilter, List<RadargunKey> localKeys, List<RadargunKey> keysMoved, RoundStats stats) {
      long duration = 0;
      int bfErrors = 0;
      for (RadargunKey key : localKeys) {
         long ts1 = System.nanoTime();
         boolean bfResult = bloomFilter.contains(key);
         long ts2 = System.nanoTime();
         duration += ts2 - ts1;
         if (bfResult && !keysMoved.contains(key)) {
            bfErrors++;
         }
      }
      stats.bloomFilterQueryTime = duration;
      stats.bloomFilterErrors = bfErrors;
   }

   private ImmutableBloomierFilter<RadargunKey, Byte> createBloomierFilter(List<RadargunKey> keyToAdd, RoundStats stats) {
      Map<RadargunKey, Byte> map = new HashMap<RadargunKey, Byte>();
      Random random = new Random();
      for (RadargunKey key : keyToAdd) {
         map.put(key, (byte)random.nextInt(40));
      }

      long ts1 = System.nanoTime();
      ImmutableBloomierFilter<RadargunKey, Byte> bloomierFilter;
      try {
         bloomierFilter = new ImmutableBloomierFilter<RadargunKey, Byte>(
               map, keyToAdd.size() * 10, 10, 16, Byte.class, 10000);
      } catch (TimeoutException e) {
         log.warn("Unable to create Bloomier Filter");
         return null;
      }
      long ts2 = System.nanoTime();
      stats.bloomierFilterCreationTime = ts2 - ts1;
      return bloomierFilter;
   }

   private void test(ImmutableBloomierFilter<RadargunKey, Byte> bloomierFilter, List<RadargunKey> localKeys,
                     List<RadargunKey> keysMoved, RoundStats stats) {
      long duration = 0;
      int bfErrors = 0;
      for (RadargunKey key : localKeys) {
         long ts1 = System.nanoTime();
         boolean bfResult = bloomierFilter.get(key) != null;
         long ts2 = System.nanoTime();
         duration += ts2 - ts1;
         if (bfResult && !keysMoved.contains(key)) {
            bfErrors++;
         }
      }
      stats.bloomierFilterQueryTime = duration;
      stats.bloomierFilterErrors = bfErrors;
   }

   private void writeHeader(BufferedWriter writer) throws IOException {

      writer.write(GNUPLOT_COMMENT);
      writer.write("roundId");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("totalKeys");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("totalKeysMoved");
      writer.write(GNUPLOT_SEPARATOR);

      writer.write("scalableBloomFilterCreationTime");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("scalableBloomFilterQueryTime");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("scalableBloomFilterErrors");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("scalableBloomFilterSize");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("incrementScalableBloomFilterSize");
      writer.write(GNUPLOT_SEPARATOR);

      writer.write("bloomFilterCreationTime");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("bloomFilterQueryTime");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("bloomFilterErrors");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("bloomFilterSize");
      writer.write(GNUPLOT_SEPARATOR);

      writer.write("bloomierFilterCreationTime");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("bloomierFilterQueryTime");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("bloomierFilterErrors");
      writer.write(GNUPLOT_SEPARATOR);
      writer.write("bloomierFilterSize");
      writer.write(GNUPLOT_SEPARATOR);

      writer.newLine();
      writer.flush();
   }

   private void write(BufferedWriter writer, RoundStats stats) throws IOException {

      writer.write(Integer.toString(stats.roundId));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Integer.toString(stats.totalKeys));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Integer.toString(stats.totalKeysMoved));
      writer.write(GNUPLOT_SEPARATOR);

      writer.write(Long.toString(stats.scalableBloomFilterCreationTime));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Long.toString(stats.scalableBloomFilterQueryTime));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Integer.toString(stats.scalableBloomFilterErrors));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Integer.toString(stats.scalableBloomFilterSize));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Integer.toString(stats.incrementScalableBloomFilterSize));
      writer.write(GNUPLOT_SEPARATOR);

      writer.write(Long.toString(stats.bloomFilterCreationTime));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Long.toString(stats.bloomFilterQueryTime));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Integer.toString(stats.bloomFilterErrors));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Integer.toString(stats.bloomFilterSize));
      writer.write(GNUPLOT_SEPARATOR);

      writer.write(Long.toString(stats.bloomierFilterCreationTime));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Long.toString(stats.bloomierFilterQueryTime));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Integer.toString(stats.bloomierFilterErrors));
      writer.write(GNUPLOT_SEPARATOR);
      writer.write(Integer.toString(stats.bloomierFilterSize));
      writer.write(GNUPLOT_SEPARATOR);

      writer.newLine();
      writer.flush();
   }

   private int serializedSize(Serializable object) {
      int size = 0;
      try {
         ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
         objectOutputStream.writeObject(object);
         objectOutputStream.flush();

         size = byteArrayOutputStream.toByteArray().length;

         byteArrayOutputStream.close();
         objectOutputStream.close();
      } catch (Exception e) {
         //no-op
      }
      return size;
   }

   private int serializedSize(ImmutableBloomierFilter<?,?> object) {
      int size = 0;
      for (byte[] array : object.getTable()) {
         size += array.length;
      }
      return size;
   }


   private List<RadargunKey> getKeys() throws IOException, ClassNotFoundException {
      ObjectInputStream objectInputStream = getObjectInputStream(keysFilePath);
      List<RadargunKey> list = new LinkedList<RadargunKey>();

      while (true) {
         try {
            Object readObject = objectInputStream.readObject();
            if (readObject instanceof RadargunKey) {
               list.add((RadargunKey) readObject);
            }
         } catch (OptionalDataException e) {
            break;
         } catch (EOFException e) {
            break;
         }
      }
      return list;
   }

   private List<Integer> getKeysPerRound() throws IOException, ClassNotFoundException {
      BufferedReader reader = getBufferedReader(roundsFilePath);
      List<Integer> list = new LinkedList<Integer>();

      String line;
      while ((line = reader.readLine()) != null) {
         try {
            list.add(Integer.parseInt(line));
         } catch (Exception e) {
            //no-op
         }
      }
      return list;
   }

   private ObjectInputStream getObjectInputStream(String filePath) throws IOException {
      return new ObjectInputStream(new FileInputStream(filePath));
   }

   private BufferedReader getBufferedReader(String filePath) throws IOException {
      return new BufferedReader(new FileReader(filePath));
   }

   private BufferedWriter getBufferedWriterForOutput() throws IOException {
      return new BufferedWriter(new FileWriter(outputFilePath));
   }

   public void setKeysFilePath(String keysFilePath) {
      this.keysFilePath = keysFilePath;
   }

   public void setRoundsFilePath(String roundsFilePath) {
      this.roundsFilePath = roundsFilePath;
   }

   public void setOutputFilePath(String outputFilePath) {
      this.outputFilePath = outputFilePath;
   }

   public void setFalsePositive(double falsePositive) {
      this.falsePositive = falsePositive;
   }

   private boolean logAndReturnError(String errorMessage) {
      log.warn(errorMessage);
      return false;
   }

   @Override
   public String toString() {
      return "BloomFilterStage{" +
            "keysFilePath='" + keysFilePath + '\'' +
            ", roundsFilePath='" + roundsFilePath + '\'' +
            ", outputFilePath='" + outputFilePath + '\'' +
            ", falsePositive=" + falsePositive +
            '}';
   }

   @Override
   public void init(MasterState masterState) {
      //nothing
   }

   @Override
   public Stage clone() {
      BloomFilterStage dolly = new BloomFilterStage();
      dolly.keysFilePath = keysFilePath;
      dolly.roundsFilePath = roundsFilePath;
      dolly.outputFilePath = outputFilePath;
      dolly.falsePositive = falsePositive;
      return dolly;
   }

   private class RoundStats {
      private int roundId;
      private int totalKeys;
      private int totalKeysMoved;

      //scalable bloom filter
      private long scalableBloomFilterCreationTime;
      private long scalableBloomFilterQueryTime;
      private int scalableBloomFilterErrors;
      private int scalableBloomFilterSize;
      private int incrementScalableBloomFilterSize;

      //ispn bloom filter
      private long bloomFilterCreationTime;
      private long bloomFilterQueryTime;
      private int bloomFilterErrors;
      private int bloomFilterSize;

      //immutable bloomier filter
      private long bloomierFilterCreationTime;
      private long bloomierFilterQueryTime;
      private int bloomierFilterErrors;
      private int bloomierFilterSize;
   }
}
