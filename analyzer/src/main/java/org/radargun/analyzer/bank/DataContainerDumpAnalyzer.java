package org.radargun.analyzer.bank;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class DataContainerDumpAnalyzer {

    public static void main(String[] args) throws Exception {
        InputStream inputStream = new FileInputStream(args[0]);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        HashMap<String, Chain> map = new HashMap<String, Chain>();

        String line;
        while ((line = reader.readLine()) != null) {
            String key = line.split("=", 2)[0];
            String valuesChain = line.split("=", 2)[1];
            process(map, key, valuesChain);
        }
        reader.close();
        for (int i = 2078; i <= 2079; ++i) {
            checkSnapshot(map, i);
        }
    }

    private static void checkSnapshot(HashMap<String, Chain> map, int snapshot) {
        int sum = 0;
        for (Map.Entry<String, Chain> entry : map.entrySet()) {
            ChainEntry chainEntry = find(entry.getValue(), snapshot);
            System.out.println(entry.getKey() + "=" + chainEntry);
            sum += chainEntry.getValue();
        }
        System.out.println("sum(" + snapshot + ")=" + sum);
    }

    private static ChainEntry find(Chain chain, int snapshot) {
        for (ChainEntry chainEntry : chain) {
            if (chainEntry.isValid(snapshot)) {
                return chainEntry;
            }
        }
        return null;
    }

    private static void process(HashMap<String, Chain> map, String key, String chain) {
        if (!key.startsWith("ACCOUNT")) {
            return;
        }
        boolean duplicate = map.containsKey(key);
        String[] valuesAndVersions = chain.split("[|]");
        Chain entryChain;
        if (duplicate) {
            entryChain = map.get(key);
        } else {
            entryChain = new Chain();
            map.put(key, entryChain);
        }
        int index = 0;
        for (String valueAndVersion : valuesAndVersions) {
            //8394=GMUCacheEntryVersion{version=1074, subVersion=0, viewId=6, cacheName=x}
            int value = Integer.parseInt(valueAndVersion.split("=", 2)[0]);
            int version = parserVersion(valueAndVersion.split("=", 2)[1]);
            if (duplicate) {
                if (!entryChain.chain.get(index).check(value, version)) {
                    throw new RuntimeException("Invalid version and/or value for key " + key);
                }
            } else {
                entryChain.chain.add(new ChainEntry(value, version));
            }
            index++;
        }
    }

    private static int parserVersion(String version) {
        int index1 = version.indexOf("=");
        int index2 = version.indexOf(",");
        if (index1 != -1 && index2 != -1 && index2 > index1) {
            return Integer.parseInt(version.substring(index1 + 1, index2));
        }
        throw new RuntimeException("Cannot parse " + version);
    }

    private static class Chain implements Iterable<ChainEntry> {
        private final List<ChainEntry> chain = new ArrayList<ChainEntry>();

        @Override
        public Iterator<ChainEntry> iterator() {
            return chain.iterator();
        }
    }

    private static class ChainEntry {
        private final int value;
        private final int version;

        private ChainEntry(int value, int version) {
            this.value = value;
            this.version = version;
        }

        public final boolean check(int value, int version) {
            return this.value == value && this.version == version;
        }

        public final boolean isValid(int version) {
            return this.version <= version;
        }

        public final int getVersion() {
            return version;
        }

        public final int getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "ChainEntry{" +
                    "value=" + value +
                    ", version=" + version +
                    '}';
        }
    }
}
