package org.radargun.cachewrappers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.radargun.stressors.ObjectKey;

import java.util.*;

/**
 * @author Mircea.Markus@jboss.com
 */
public class EvenSpreadingConsistentHash implements ConsistentHash {

    private static Log log = LogFactory.getLog(EvenSpreadingConsistentHash.class);

    /**
     * Why static? because the consistent hash is recreated when cluster changes and there's no other way to pass these
     * across
     */
    private volatile static int threadCountPerNode = -1;
    private volatile static int keysPerThread = -1;
    private volatile DefaultConsistentHash existing;


    public EvenSpreadingConsistentHash() {//needed for UT
        existing = new DefaultConsistentHash();
    }


    @Override
    public List<Address> locate(Object key, int replCount) {
        if(! (key instanceof ObjectKey)) {
            if (log.isTraceEnabled()) log.trace("Delegating key " + key + " to default CH");
            return existing.locate(key, replCount);
        }

        if (threadCountPerNode <= 0 || keysPerThread <= 0) throw new IllegalStateException("keysPerThread and threadCountPerNode need to be set!");

        int clusterSize = existing.getCaches().size();

        int keyIndexInCluster = getSequenceNumber((ObjectKey) key);
        int firstIndex = keyIndexInCluster % existing.getCaches().size();

        List<Address> result = new ArrayList<Address>();
        List<Address> caches = new ArrayList<Address>(existing.getCaches());
        for (int i = 0; i < replCount; i++) {
            Address address = caches.get((firstIndex + i) % clusterSize);
            result.add(address);
            if (result.size() == replCount) break;
        }
        if (log.isTraceEnabled())
            log.trace("Handling key " + key + ", clusterIndex==" + keyIndexInCluster +" and EvenSpreadingConsistentHash --> " + result);

        return Collections.unmodifiableList(result);
    }

    private int getSequenceNumber(ObjectKey key) {
        return key.getKeyIndexInCluster(threadCountPerNode, keysPerThread);
    }

    public void init(int threadCountPerNode, int keysPerThread) {
        log.trace("Setting threadCountPerNode =" + threadCountPerNode + " and keysPerThread = " + keysPerThread);
        this.threadCountPerNode = threadCountPerNode;
        this.keysPerThread = keysPerThread;
    }


    //following methods should only be used during rehashing, so no point in implementing them


    @Override
    public List<Address> getStateProvidersOnLeave(Address leaver, int replCount) {
        return existing.getStateProvidersOnLeave(leaver, replCount);
    }

    @Override
    public List<Address> getStateProvidersOnJoin(Address joiner, int replCount) {
        return existing.getStateProvidersOnJoin(joiner, replCount);
    }

    @Override
    public List<Address> getBackupsForNode(Address node, int replCount) {
        if (log.isTraceEnabled()) log.trace("getBackupsForNode (" + node +")");
        return existing.getBackupsForNode(node, replCount);
    }

    /*@Override
    public void setTopologyInfo(TopologyInfo topologyInfo) {
        throw new UnsupportedOperationException("")
    }*/

    public Map<Object, List<Address>> locateAll(Collection<Object> keys, int replCount) {
        Map<Object, List<Address>> locations = new HashMap<Object, List<Address>>();
        for (Object k : keys) locations.put(k, locate(k, replCount));
        return locations;
    }

    public boolean isKeyLocalToAddress(Address a, Object key, int replCount) {
        // simple, brute-force impl
        return locate(key, replCount).contains(a);
    }

    @Override
    public List<Integer> getHashIds(Address a) {
        return existing.getHashIds(a);
    }

    @Override
    public void setCaches(Set<Address> caches) {
        existing.setCaches(caches);
    }

    @Override
    public Set<Address> getCaches() {
        return existing.getCaches();
    }
}
