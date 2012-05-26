package org.radargun.keygenerator;

import java.util.Iterator;

/**
 * The key iterator for the warmup phase, that can rollback the iterator to a previous checkpoint
 *
 * @author Pedro Ruivo
 * @since 1.1
 */
public interface WarmupIterator extends Iterator<Object> {

   /**
    * set the last checkpoint, that the iterator can return if {@link #returnToLastCheckpoint()} is invoked
    */
   void setCheckpoint();

   /**
    * returns to the last checkpoint set by {@link #setCheckpoint()} 
    */
   void returnToLastCheckpoint();

   /**
    * * returns a random value to set to a key. the size of the value is specify in the construction of the instance
    *
    * @return  a random value to set to a key    
    */
   String getRandomValue();

   /**
    * returns the bucket prefix
    *
    * @return  the bucket prefix
    */
   String getBucketPrefix();
}
