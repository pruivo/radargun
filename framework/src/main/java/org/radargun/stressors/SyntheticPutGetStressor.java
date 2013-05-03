package org.radargun.stressors;/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author Diego Didona, didona@gsd.inesc-id.pt
 *         Date: 20/03/13
 */
public class SyntheticPutGetStressor extends PutGetStressor {
    private int readOnlyXactSize = 1;
    private int updateXactWrites = 1;
    private int updateXactReads = 1;
    private boolean allowBlindWrites = false;
    private long startTime;

    public boolean isAllowBlindWrites() {
        return allowBlindWrites;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }



    public int getupdateXactWrites() {
        return updateXactWrites;
    }

    public int getReadOnlyXactSize() {
        return readOnlyXactSize;
    }

    public void setReadOnlyXactSize(int readOnlyXactSize) {
        this.readOnlyXactSize = readOnlyXactSize;
    }

    public int getUpdateXactWrites() {
        return updateXactWrites;
    }

    public void setUpdateXactWrites(int updateXactWrites) {
        this.updateXactWrites = updateXactWrites;
    }

    public int getUpdateXactReads() {
        return updateXactReads;
    }

    public void setUpdateXactReads(int updateXactReads) {
        this.updateXactReads = updateXactReads;
    }

    public void setupdateXactWrites(int numWrites) {
        this.updateXactWrites = numWrites;
    }

    protected Map<String, String> processResults(List<Stressor> stressors) {
        long duration = 0;
        int reads = 0;
        int writes = 0;
        int localFailures = 0;
        int remoteFailures = 0;
        duration = (long) (1e-6 * (System.nanoTime() - startTime));
        for (Stressor stressorrrr : stressors) {
            SyntheticStressor stressor = (SyntheticStressor) stressorrrr;
            reads += stressor.reads;
            writes += stressor.writes;
            localFailures += stressor.localAborts;
            remoteFailures += stressor.remoteAborts;
        }

        Map<String, String> results = new LinkedHashMap<String, String>();
        results.put("DURATION", str(duration));
        results.put("REQ_PER_SEC", str((reads + writes) / duration));
        results.put("READ_COUNT", str(reads));
        results.put("WRITE_COUNT", str(writes));
        results.put("LOCAL_FAILURES", str(localFailures));
        results.put("REMOTE_FAILURES", str(remoteFailures));
        results.putAll(cacheWrapper.getAdditionalStats());
        return results;

    }


    @Override
    protected List<Stressor> executeOperations() throws Exception {
        List<Stressor> stressors = new ArrayList<Stressor>(numOfThreads);
        startPoint = new CountDownLatch(1);
        startTime = System.nanoTime();
        for (int threadIndex = 0; threadIndex < numOfThreads; threadIndex++) {
            Stressor stressor = new SyntheticStressor(threadIndex, keyGenerator, nodeIndex, numberOfKeys);
            stressors.add(stressor);
            stressor.start();
        }
        log.info("Cache wrapper info is: " + cacheWrapper.getInfo());

        startPoint.countDown();
        log.info("Started " + stressors.size() + " stressor threads.");
        for (Stressor stressor : stressors) {
            stressor.join();
        }
        return stressors;
    }

    private class SyntheticStressor extends Stressor {


        private KeyGenerator keyGen;
        private int nodeIndex, threadIndex, numKeys;
        private long writes, reads, localAborts, remoteAborts;

        SyntheticStressor(int threadIndex, KeyGenerator keyGen, int nodeIndex, int numKeys) {
            super(threadIndex);
            this.keyGen = keyGen;
            this.nodeIndex = nodeIndex;
            this.threadIndex = threadIndex;
            this.numKeys = numKeys;
        }

        @Override
        public void run() {
            try {
                runInternal();
            } catch (Exception e) {
                log.error("Unexpected error in stressor!", e);
            }
        }

        private void runInternal() {
            Random r = new Random();
            xactClass lastClazz = xactClass.RO;
            result outcome;
            try {
                startPoint.await();
                log.trace("Starting thread: " + getName());
            } catch (InterruptedException e) {
                log.warn(e);
            }

            while (completion.moreToRun()) {
                try {
                    lastClazz = xactClass(r);
                    outcome = doXact(r, lastClazz);
                } catch (Exception e) {
                    log.warn("Unexpected exception" + e.getMessage());
                    outcome = result.OTHER;
                }
                switch (outcome) {
                    case COM: {
                        sampleCommit(lastClazz);
                        break;
                    }
                    case AB_L: {
                        sampleLocalAbort(lastClazz);
                        break;
                    }
                    case AB_R: {
                        sampleRemoteAbort(lastClazz);
                    }
                    default: {
                        log.error("I got strange exception for class " + lastClazz);
                    }
                }
            }
        }

        private void sampleCommit(xactClass clazz) {
            switch (clazz) {
                case RO: {
                    reads++;
                    break;
                }
                case WR: {
                    writes++;
                    break;
                }
                default:
                    throw new RuntimeException("Unknown xactClass " + clazz);
            }
        }

        private void sampleLocalAbort(xactClass clazz) {
            switch (clazz) {
                case WR: {
                    localAborts++;
                    break;
                }
                default:
                    throw new RuntimeException("Xact class " + clazz + " should not abort");
            }
        }

        private void sampleRemoteAbort(xactClass clazz) {
            switch (clazz) {
                case WR: {
                    remoteAborts++;
                    break;
                }
                default:
                    throw new RuntimeException("Xact class " + clazz + " should not abort");
            }
        }


        private boolean readOnlyXact(Random r, int writePerc) {
            return !cacheWrapper.isTheMaster() || r.nextInt(100) > writePerc;
        }


        private void doReadXact(Random r) throws Exception {
            int doneRead = 0;
            while (doneRead++ < readOnlyXactSize) {
                doOp(false, r.nextInt(numKeys));
            }
        }

        private void doWriteXact(Random r) throws Exception {
            int toDoRead = updateXactReads, toDoWrite = updateXactWrites, toDo = updateXactWrites + updateXactReads, writePerc = 100 * (int) (((double) updateXactWrites) / ((double) (toDo)));
            boolean doPut;
            int[] readSet = new int[updateXactReads];
            int readSetIndex = -1, lastWriteIndex = -1;
            boolean canWrite = false;
            int keyToAccess;
            while (toDo > 0) {
                keyToAccess = r.nextInt(numKeys);
                if (toDo == toDoWrite)      //I have only puts left
                    doPut = true;
                else if (toDo == toDoRead)  //I have only reads left
                    doPut = false;
                else if (allowBlindWrites) {     //I choose uniformly
                    doPut = r.nextInt(100) < writePerc;
                } else {
                    if (!canWrite) {
                        doPut = false;
                        readSet[++readSetIndex] = keyToAccess;
                        canWrite = true;
                    } else {
                        doPut = r.nextInt(100) < writePerc;
                        if (doPut) {
                            keyToAccess = readSet[readSetIndex];     //Access the last value read. This is the simplest way to avoid blindWrites, still trying to avoid reading always the same item
                        } else {
                            readSet[++readSetIndex] = keyToAccess;
                        }
                    }
                }
                doOp(doPut, keyToAccess);
                toDo--;
                if (doPut) {
                    toDoWrite--;
                } else {
                    toDoRead--;
                }
            }

        }

        private void doOp(boolean put, int keyIndex) throws Exception {
            if (put)
                cacheWrapper.put(null, keyGen.generateKey(nodeIndex, threadIndex, keyIndex), generateRandomString(sizeOfValue));
            else
                cacheWrapper.get(null, keyGen.generateKey(nodeIndex, threadIndex, keyIndex));
        }


        private xactClass xactClass(Random r) {
            if (readOnlyXact(r, writePercentage))
                return xactClass.RO;
            return xactClass.WR;
        }


        private result doXact(Random r, xactClass clazz) throws Exception {
            cacheWrapper.startTransaction();
            try {
                switch (clazz) {
                    case RO: {
                        doReadXact(r);
                        break;
                    }
                    case WR: {
                        doWriteXact(r);
                        break;
                    }
                    default:
                        throw new RuntimeException("Invalid xact clazz " + clazz);
                }
            } catch (Exception e) {
                log.trace("Rollback while running locally");
                cacheWrapper.endTransaction(false);
                return result.AB_L;
            }

            try {
                cacheWrapper.endTransaction(true);
            } catch (Exception e) {
                log.trace("Rollback at prepare time");
                return result.AB_R;
            }
            return result.COM;
        }


    }

    public void setAllowBlindWrites(boolean allowwBlindWrites) {
        this.allowBlindWrites = allowwBlindWrites;
    }

    private enum xactClass {
        RO, WR
    }

    private enum result {
        AB_L, AB_R, COM, OTHER
    }


}
