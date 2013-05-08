package org.radargun.stages.synthetic;

import org.radargun.reporting.PutGetChartGenerator;
import org.radargun.stressors.KeyGenerator;
import org.radargun.stressors.PutGetStressor;

import java.util.List;
import java.util.Random;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class SyntheticXactFactory extends XactFactory<SyntheticXactParams, SyntheticXact> {

   public SyntheticXactFactory(SyntheticXactParams params) {
      super(params);
   }

   @Override
   public SyntheticXact buildXact(SyntheticXact last) {

      XACT_RETRY retry = params.getXact_retry();
      SyntheticXact toRet = null;
      xactClass clazz;
      XactOp[] ops;
      if (retry == XACT_RETRY.NO_RETRY || last == null || last.isCommit()) {    //brand new xact

         clazz = computeClazz();
         if (clazz == xactClass.RO) {
            ops = buildReadSet();
         } else {
            ops = buildReadWriteSet();
         }
         toRet = new SyntheticXact();
         toRet.setOps(ops);
         toRet.setClazz(clazz);
      }

      //retried xact
      else if (retry == XACT_RETRY.RETRY_SAME_CLASS) {
         toRet = new SyntheticXact(last,true);
      }
      else{
         toRet = new SyntheticXact(last,false);
      }
      return  toRet;
   }

   private xactClass computeClazz() {
      if (params.getRandom().nextInt(100) < params.getWritePercentage())
         return xactClass.WR;
      return xactClass.RO;
   }


   private XactOp[] buildReadWriteSet() {
      int toDoRead = params.getUpReads(), toDoWrite = params.getUpPuts(), toDo = toDoRead + toDoWrite, writePerc = 100 * (int) (((double) toDoWrite) / ((double) (toDo)));
      Random r = params.getRandom();
      int numKeys = params.getNumKeys();
      boolean allowBlindWrites = params.isAllowBlindWrites();
      XactOp[] ops = new XactOp[toDo];
      boolean doPut;
      int size = params.getSizeOfValue();
      boolean canWrite = false;
      int keyToAccess;
      int lastRead = 0;
      for (int i = 0; i < toDo; i++) {
         //Determine if you can read or not

         if (toDo == toDoWrite)      //I have only puts left
            doPut = true;
         else if (toDo == toDoRead)  //I have only reads left
            doPut = false;
         else if (allowBlindWrites) {     //I choose uniformly
            doPut = r.nextInt(100) < writePerc;
         } else {
            if (!canWrite) {
               doPut = false;
               canWrite = true;
            } else {
               doPut = r.nextInt(100) < writePerc;
            }
         }
         if (doPut) {  //xact can write
            if (allowBlindWrites) { //xact can choose what it wants
               keyToAccess = r.nextInt(numKeys);
            } else { //xact has to write something it already read
               keyToAccess = lastRead;
            }
         } else {    //xact reads
            keyToAccess = r.nextInt(numKeys);
            lastRead = keyToAccess;
         }
         ops[i] = new XactOp(keyToAccess, generateRandomString(size), doPut);
         toDo--;
         if (doPut) {
            toDoWrite--;
         } else {
            toDoRead--;
         }
      }
      return ops;
   }

   private XactOp[] buildReadSet() {
      int numR = params.getROGets();
      XactOp[] ops = new XactOp[numR];
      KeyGenerator keyGen = params.getKeyGenerator();
      Object key;
      int keyToAccess;
      Random r = params.getRandom();
      int size = params.getSizeOfValue();
      int numKeys = params.getNumKeys(), nodeIndex = params.getNodeIndex(), threadIndex = params.getThreadIndex();

      for (int i = 0; i < numR; i++) {
         keyToAccess = r.nextInt(numKeys);
         key = keyGen.generateKey(nodeIndex, threadIndex, keyToAccess);
         ops[i] = new XactOp(key, generateRandomString(size), false);
      }
      return ops;
   }

   private String generateRandomString(int size) {
      // each char is 2 bytes
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < size / 2; i++) sb.append((char) (64 + params.getRandom().nextInt(26)));
      return sb.toString();
   }
}
