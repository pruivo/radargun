package org.radargun.state;

import org.radargun.CacheWrapper;
import org.radargun.workloads.KeyGeneratorFactory;

import java.net.InetAddress;

/**
 * State residing on slave, passed to each's {@link org.radargun.DistStage#initOnSlave(SlaveState)}
 *
 * @author Mircea.Markus@jboss.com
 */
public class SlaveState extends StateBase {

   private InetAddress masterAddress;
   private InetAddress localAddress;
   private CacheWrapper cacheWrapper;
   private KeyGeneratorFactory keyGeneratorFactory;

   public InetAddress getMasterAddress() {
      return masterAddress;
   }

   public void setMasterAddress(InetAddress serverAddress) {
      this.masterAddress = serverAddress;
   }

   public InetAddress getLocalAddress() {
      return localAddress;
   }

   public void setLocalAddress(InetAddress localAddress) {
      this.localAddress = localAddress;
   }

   public void setCacheWrapper(CacheWrapper wrapper) {
      this.cacheWrapper = wrapper;
   }

   public CacheWrapper getCacheWrapper() {
      return cacheWrapper;
   }

   public KeyGeneratorFactory getKeyGeneratorFactory() {
      return keyGeneratorFactory;
   }

   public void setKeyGeneratorFactory(KeyGeneratorFactory keyGeneratorFactory) {
      this.keyGeneratorFactory = keyGeneratorFactory;
   }
}
