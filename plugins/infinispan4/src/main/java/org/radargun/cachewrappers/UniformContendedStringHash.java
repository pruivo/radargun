package org.radargun.cachewrappers;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.util.*;

/**
 * @author Diego Didona  - didona@gsd.inesc-id.pt
 *         Since 10/04/13
 */
public class UniformContendedStringHash implements ConsistentHash {

    private Address[] caches;

    public UniformContendedStringHash(Set<Address> caches) {
        setCaches(caches);
    }


    public void setCaches(Set<Address> caches) {
        this.caches = new Address[caches.size()];
        int i = 0;
        for (Address c : caches)
            this.caches[i++] = c;
    }


    public Set<Address> getCaches() {
        Set ret = new HashSet<Address>();
        Collections.addAll(ret, caches);
        return ret;
    }


    public List<Address> locate(Object key, int replCount) {
        List<Address> ret = new LinkedList<Address>();
        rollAndFill(ret, replCount, (double) keyIndex(key), (double) this.caches.length);
        return ret;
    }

    private long keyIndex(Object key) {
        String s = (String) key;
        return Long.parseLong(s.split("_")[2]);
    }


    private void rollAndFill(List<Address> list, int replCount, double keyIndex, double numNodes) {
        int node = (int) (keyIndex % numNodes);
        for (int i = 0; i < replCount; i++) {
            list.add(this.caches[node + i]);
            if (i == numNodes - 1) //roll
                node = 0;
        }
    }


    public Map<Object, List<Address>> locateAll(Collection<Object> keys, int replCount) {
        Map<Object, List<Address>> ret = new HashMap<Object, List<Address>>();
        for (Object o : keys)
            ret.put(o, locate(o, replCount));
        return ret;
    }


    public boolean isKeyLocalToAddress(Address a, Object key, int replCount) {
        return locate(key, replCount).contains(a);
    }


    public List<Integer> getHashIds(Address a) {
        return new LinkedList<Integer>();
    }


    public Address primaryLocation(Object key) {
        int index = (int) (((double) keyIndex(key)) / (double) caches.length);
        return caches[index];
    }

    @Override
    public int getNumOwners() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Hash getHashFunction() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getNumSegments() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Address> getMembers() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Address locatePrimaryOwner(Object o) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Address> locateOwners(Object o) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<Address> locateAllOwners(Collection<Object> objects) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isKeyLocalToNode(Address address, Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getSegment(Object o) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Address> locateOwnersForSegment(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Address locatePrimaryOwnerForSegment(int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<Integer> getSegmentsForOwner(Address address) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getRoutingTableAsString() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
