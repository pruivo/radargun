package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.keygen2.RadargunKey;
import org.radargun.reporting.DataPlacementStats;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OptionalDataException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.radargun.reporting.DataPlacementStats.writeHeader;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public class DataPlacementStage extends AbstractDistStage {

   private String keysFilePath = null;
   private String keysMovedFormat = null;
   private String outputFilePath = null;

   @Override
   public DistStageAck executeOnSlave() {
      DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
      CacheWrapper cacheWrapper = slaveState.getCacheWrapper();

      if (cacheWrapper == null) {
         return logAndReturnError("Cache Wrapper not configured!", result);
      } else if (keysFilePath == null) {
         return logAndReturnError("Keys File Path not defined!", result);
      } else if (keysMovedFormat == null) {
         return logAndReturnError("Keys Moved Format not defined!", result);
      } else if (outputFilePath == null) {
         return logAndReturnError("Output File Path not defined!", result);
      }

      try {
         log.info("Reading keys...");
         BufferedWriter writer = getBufferedWriterForOutput();
         Collection<RadargunKey> initialKeys = getKeys();
         log.info(initialKeys.size() + " keys read");
         log.info("Writing header in file");
         writeHeader(writer);

         int roundId = 1;
         while (true) {
            String path = String.format(keysMovedFormat, roundId);
            if (new File(path).exists()) {
               log.info("found " + path + ". processing...");
               DataPlacementStats stats = processRound(roundId, path, initialKeys, cacheWrapper);
               stats.writeValues(writer);
               log.info("found " + path + ". done!");
            } else {
               log.info("not found " + path + ". breaking loop!");
               break;
            }
            roundId++;
         }

      } catch (Exception e) {
         e.printStackTrace();
         return logAndReturnError("Exception caught: " + e.getMessage(), result);
      }

      log.info("all done!");
      return result;
   }

   private DataPlacementStats processRound(int roundId, String path, Collection<RadargunKey> initialKeys,
                                           CacheWrapper cacheWrapper) throws Exception {
      DataPlacementStats stats = new DataPlacementStats(roundId);
      ObjectInputStream objectInputStream = getObjectInputStream(path);
      cacheWrapper.collectDataPlacementStats(objectInputStream, initialKeys, stats);
      return stats;
   }

   private Collection<RadargunKey> getKeys() throws IOException, ClassNotFoundException {
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

   private ObjectInputStream getObjectInputStream(String filePath) throws IOException {
      return new ObjectInputStream(new FileInputStream(filePath));
   }

   private BufferedWriter getBufferedWriterForOutput() throws IOException {
      return new BufferedWriter(new FileWriter(outputFilePath));
   }

   public void setKeysFilePath(String keysFilePath) {
      this.keysFilePath = keysFilePath;
   }

   public void setKeysMovedFormat(String keysMovedFormat) {
      this.keysMovedFormat = keysMovedFormat;
   }

   public void setOutputFilePath(String outputFilePath) {
      this.outputFilePath = outputFilePath;
   }

   private DistStageAck logAndReturnError(String errorMessage, DefaultDistStageAck ack) {
      log.warn(errorMessage);
      ack.setError(true);
      ack.setErrorMessage(errorMessage);
      return ack;
   }

   @Override
   public String toString() {
      return "DataPlacementStage{" +
            "keysFilePath='" + keysFilePath + '\'' +
            ", keysMovedFormat='" + keysMovedFormat + '\'' +
            ", outputFilePath='" + outputFilePath + '\'' +
            ", " + super.toString();
   }
}
