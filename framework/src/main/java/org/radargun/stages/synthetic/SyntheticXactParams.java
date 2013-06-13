package org.radargun.stages.synthetic;

import org.radargun.CacheWrapper;
import org.radargun.stressors.KeyGenerator;

import java.util.Random;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticXactParams extends XactParam {

   private boolean allowBlindWrites;
   private Random random;
   private XACT_RETRY xact_retry;
   private int ROGets;
   private int UpPuts;
   private int UpReads;
   private KeyGenerator keyGenerator;
   private int nodeIndex, threadIndex, numKeys, sizeOfValue, writePercentage;
   private CacheWrapper cache;
   private int readsBeforeFirstWrite;

   public int getReadsBeforeFirstWrite() {
      return readsBeforeFirstWrite;
   }

   public void setReadsBeforeFirstWrite(int firstWriteIndex) {
      this.readsBeforeFirstWrite = firstWriteIndex;
   }

   public void setCache(CacheWrapper cache) {
      this.cache = cache;
   }

   public CacheWrapper getCache() {
      return cache;
   }

   public int getWritePercentage() {
      return writePercentage;
   }

   public void setWritePercentage(int writePercentage) {
      this.writePercentage = writePercentage;
   }

   public int getSizeOfValue() {
      return sizeOfValue;
   }

   public void setSizeOfValue(int sizeOfValue) {
      this.sizeOfValue = sizeOfValue;
   }

   public int getNumKeys() {
      return numKeys;
   }

   public void setNumKeys(int numKeys) {
      this.numKeys = numKeys;
   }

   public int getNodeIndex() {
      return nodeIndex;
   }

   public void setNodeIndex(int nodeIndex) {
      this.nodeIndex = nodeIndex;
   }

   public int getThreadIndex() {
      return threadIndex;
   }

   public void setThreadIndex(int threadIndex) {
      this.threadIndex = threadIndex;
   }

   public KeyGenerator getKeyGenerator() {
      return keyGenerator;
   }

   public void setKeyGenerator(KeyGenerator keyGenerator) {
      this.keyGenerator = keyGenerator;
   }

   public int getROGets() {
      return ROGets;
   }

   public void setROGets(int ROGets) {
      this.ROGets = ROGets;
   }

   public int getUpPuts() {
      return UpPuts;
   }

   public void setUpPuts(int upPuts) {
      UpPuts = upPuts;
   }

   public int getUpReads() {
      return UpReads;
   }

   public void setUpReads(int upReads) {
      UpReads = upReads;
   }

   public boolean isAllowBlindWrites() {
      return allowBlindWrites;
   }

   public void setAllowBlindWrites(boolean allowBlindWrites) {
      this.allowBlindWrites = allowBlindWrites;
   }

   public Random getRandom() {
      return random;
   }

   public void setRandom(Random random) {
      this.random = random;
   }

   public XACT_RETRY getXact_retry() {
      return xact_retry;
   }

   public void setXact_retry(XACT_RETRY xact_retry) {
      this.xact_retry = xact_retry;
   }

   @Override
   public String toString() {
      return "SyntheticXactParams{" +
            "allowBlindWrites=" + allowBlindWrites +
            ", random=" + random +
            ", xact_retry=" + xact_retry +
            ", ROGets=" + ROGets +
            ", UpPuts=" + UpPuts +
            ", UpReads=" + UpReads +
            ", keyGenerator=" + keyGenerator +
            ", nodeIndex=" + nodeIndex +
            ", threadIndex=" + threadIndex +
            ", numKeys=" + numKeys +
            ", sizeOfValue=" + sizeOfValue +
            ", writePercentage=" + writePercentage +
            ", cache=" + cache +
            '}';
   }
}
