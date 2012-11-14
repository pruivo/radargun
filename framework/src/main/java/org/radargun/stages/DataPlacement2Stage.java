package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.keygen2.RadargunKey;
import org.radargun.reporting.DataPlacementStats;

import java.io.BufferedWriter;
import java.io.Closeable;
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
public class DataPlacement2Stage extends AbstractDistStage {

   private String keysFilePath = null;
   private String keysMovedFormat = null;

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
      }

      try {
         log.info("Reading keys...");
         BufferedWriter writer = getBufferedWriter(keysFilePath + ".txt");
         Collection<RadargunKey> initialKeys = getKeys();
         log.info(initialKeys.size() + " keys read");
         log.info("Writing keys...");

         for (RadargunKey key : initialKeys) {
            writer.write(key.toString());
            writer.newLine();
            writer.flush();
         }
         safeClose(writer);


         int roundId = 1;
         while (true) {
            String path = String.format(keysMovedFormat, roundId);
            if (new File(path).exists()) {
               log.info("found " + path + ". processing...");
               writer = getBufferedWriter(path + ".txt");
               cacheWrapper.convertTotString(getObjectInputStream(path), writer);
               safeClose(writer);
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

   private void safeClose(Closeable closeable) {
      try {
         closeable.close();
      } catch (Exception e) {
         //no-op
      }
   }

   private BufferedWriter getBufferedWriter(String filePath) {
      try {
         return new BufferedWriter(new FileWriter(filePath));
      } catch (Exception e) {
         e.printStackTrace();
         return null;
      }
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

   public void setKeysFilePath(String keysFilePath) {
      this.keysFilePath = keysFilePath;
   }

   public void setKeysMovedFormat(String keysMovedFormat) {
      this.keysMovedFormat = keysMovedFormat;
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
            ", " + super.toString();
   }
}
