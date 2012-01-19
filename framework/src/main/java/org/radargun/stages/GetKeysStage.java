package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;

/**
 * Date: 1/18/12
 * Time: 6:19 PM
 *
 * @author pruivo
 */
public class GetKeysStage extends AbstractDistStage {

    private String fileNameFormat = "keys-%n-%h%m";

    @Override
    public DistStageAck executeOnSlave() {
        DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
        CacheWrapper cacheWrapper = slaveState.getCacheWrapper();
        if (cacheWrapper == null) {
            log.info("Not running test on this slave as the wrapper hasn't been configured.");
            return result;
        }

        log.info("Starting GetKeysStage:" + this.toString());
        String fileName = "./" + getFileName();
        log.info("Save keys to file: " + fileName);

        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
            for (Map.Entry<String, SortedSet<String>> bucketAndKeys : cacheWrapper.getStressedKeys().getEntrySet()) {
                String bucked = bucketAndKeys.getKey();
                bw.append("================= BUCKET [")
                        .append(bucked)
                        .append("] ==============");
                bw.newLine();
                for (String key : bucketAndKeys.getValue()) {
                    if (cacheWrapper.isKeyLocal(key)) {
                        bw.append(key);
                        bw.append("=>");
                        bw.append(String.valueOf(cacheWrapper.get(bucked, key)));
                        bw.newLine();
                        bw.flush();
                    }
                }
            }
            bw.flush();
            bw.close();
        } catch(Exception e) {
            log.warn("Error while saving keys to file " + e);
        }

        return result;
    }

    @Override
    public String toString() {
        return "GetKeysStage {" +
                "fileNameFormat=" + fileNameFormat +
                ", " + super.toString();
    }

    public void setFileNameFormat(String fileNameFormat) {
        this.fileNameFormat = fileNameFormat;
    }

    private String getFileName() {
        Map<String, String> attributes = new HashMap<String, String>(7);
        //hostname
        attributes.put("%n",this.slaveState.getLocalAddress().getHostName());
        Date now = new Date();

        //day of month
        attributes.put("%D", String.valueOf(now.getDate()));
        //month
        attributes.put("%M", String.valueOf(now.getMonth() + 1));
        //year
        attributes.put("%Y", String.valueOf(now.getYear() + 1900));

        //hour
        attributes.put("%h", String.valueOf(now.getHours()));
        //minute
        attributes.put("%m", String.valueOf(now.getMinutes()));
        //seconds
        attributes.put("%s", String.valueOf(now.getSeconds()));

        String fileName = fileNameFormat;
        for (Map.Entry<String, String> attr : attributes.entrySet()) {
            fileName = fileName.replaceAll(attr.getKey(), attr.getValue());
        }

        return fileName;
    }
}
