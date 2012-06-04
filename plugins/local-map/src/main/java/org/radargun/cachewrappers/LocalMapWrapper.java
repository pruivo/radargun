package org.radargun.cachewrappers;

import org.radargun.CacheWrapper;

import javax.transaction.RollbackException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class LocalMapWrapper implements CacheWrapper {
    private final Map<Object, Object> cache = new ConcurrentHashMap<Object, Object>();
    private final ReentrantLock txLock = new ReentrantLock();

    @Override
    public void setUp(String s, boolean b, int i) throws Exception {

    }

    @Override
    public void tearDown() throws Exception {
        empty();
    }

    @Override
    public void put(String s, Object o, Object o1) throws Exception {
        cache.put(o, o1);
    }

    @Override
    public Object get(String s, Object o) throws Exception {
        return cache.get(o);
    }

    @Override
    public void empty() throws Exception {
        cache.clear();
    }

    @Override
    public int getNumMembers() {
        return 1;
    }

    @Override
    public String getInfo() {
        return "Local Map with " + cache.size() + " entries";
    }

    @Override
    public Object getReplicatedData(String s, String s1) throws Exception {
        return cache.get(s1);
    }

    @Override
    public Object startTransaction() {
        txLock.lock();
        return null;
    }

    @Override
    public void endTransaction(boolean b) throws RollbackException {
        txLock.unlock();
    }

    @Override
    public boolean isPassiveReplication() {
        return false;
    }

    @Override
    public boolean isPrimary() {
        return false;
    }

    @Override
    public Map<String, Object> dumpTransportStats() {
        return Collections.emptyMap();
    }

    @Override
    public boolean isKeyLocal(Object o) {
        return cache.containsKey(o);
    }

    @Override
    public String getCacheMode() {
        return "none";
    }

    @Override
    public boolean isPartialReplication() {
        return false;
    }

    @Override
    public String getIDName() {
        return LocalMapWrapper.class.getName();
    }

    @Override
    public Map<Object, String> getKeysLocation(Set<Object> objects) {
        return Collections.emptyMap();
    }
}
