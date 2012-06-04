package org.radargun.utils;

import java.util.*;

/**
 * @author pedro
 *         Date: 28-05-2011
 */
public class KeyGenerator {
    //information
    private int writePercentage, minNumOp, maxNumOp;
    private List<String> poolKeys;

    //to generate keys
    private List<Boolean> isWrite;
    private List<Object> keysToRead;
    private List<Object> keysToWrite;

    //to generate the keys
    private Random random;

    public KeyGenerator(int writePercentage, List<String> poolKeys, int minNumOp, int maxNumOp) {
        this.writePercentage = writePercentage;
        this.maxNumOp = maxNumOp;
        this.minNumOp = minNumOp;
        isWrite = new LinkedList<Boolean>();
        keysToRead = new LinkedList<Object>();
        keysToWrite = new LinkedList<Object>();
        random = new Random();
        this.poolKeys = poolKeys;
    }

    public void createNewDataAccessSet() {
        clearLists();
        int numOp = getNumOfOp();
        random.setSeed(System.nanoTime());
        boolean hasWrite = false;

        while(numOp-- > 0) {
            Object key = getKey(random.nextInt(poolKeys.size()));
            int write = random.nextInt(100);
            if(write <= writePercentage || (numOp == 0 && !hasWrite)) {
                keysToWrite.add(key);
                hasWrite = true;
                isWrite.add(Boolean.TRUE);
            } else {
                keysToRead.add(key);
                isWrite.add(Boolean.FALSE);
            }
        }
    }

    public Set<Object> getWriteSet() {
        return new HashSet<Object>(keysToWrite);
    }

    public Set<Object> getReadSet() {
        return new HashSet<Object>(keysToRead);
    }

    public Iterator<Entry> getNewIterator() {
        return new KeyIterator(this);
    }

    public int numOfOperations() {
        return isWrite.size();
    }

    private void clearLists() {
        isWrite.clear();
        keysToRead.clear();
        keysToWrite.clear();
    }

    private Object getKey(int idx) {
        return poolKeys.get(idx);
    }

    private int getNumOfOp() {
        if(minNumOp == maxNumOp)
            return minNumOp;
        return (random.nextInt(Math.abs(maxNumOp - minNumOp)) + minNumOp);
    }


    public static class Entry {
        private boolean isWrite;
        private Object key;

        private Entry(Object key, boolean isWrite) {
            this.key = key;
            this.isWrite = isWrite;
        }

        public Object getKey() {
            return key;
        }

        public boolean isWrite() {
            return isWrite;
        }
    }

    private class KeyIterator implements Iterator<Entry> {
        private KeyGenerator list;
        private int pos, wpos, rpos;

        public KeyIterator(KeyGenerator keyGen) {
            this.list = keyGen;
            pos = wpos = rpos = 0;
        }

        @Override
        public boolean hasNext() {
            return pos < list.isWrite.size();
        }

        @Override
        public Entry next() {
            if(!hasNext()) {
                throw new NoSuchElementException();
            }
            boolean isWrite = list.isWrite.get(pos++);
            Object key;
            try {
                if(isWrite) {
                    key = list.keysToWrite.get(wpos++);
                } else {
                    key = list.keysToRead.get(rpos++);
                }
            } catch(IndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
            return new Entry(key, isWrite);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
