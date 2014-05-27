package com.betfair.caching;

import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 *   Make a synchronous read-through cache able to behave generally like an asynchronous one
 *
 *   The aim is to not block the caller thread on read through,
 *
 *   How: check whether an item is in the cache before calling the read-through get() on it
 *   This isn't perfect because the containsKey() then get() are not in an atomic unit
 *   so there is a chance that a few getAsync()'s will stop waiting for readthrough
 *   The vast majority won't though - you need an invalidate or invalidateAll to occur between those two lines executing
 *   for it to accidentally read-through.
 */
@ManagedResource
public class AsyncInspectableCacheAdaptor<K, V> implements AsyncCache<K, V> {

    private final InspectableCache<K, V> cache;
    private final Executor executor;
    private final AtomicLong numDirect;
    private final AtomicLong numDeferred;
    private final AtomicLong numRemovedNulls;
    private volatile boolean removeNulls;
    private volatile boolean alwaysChangeThread;

    public AsyncInspectableCacheAdaptor(InspectableCache<K, V> synchCache, Executor executor) {
        this.cache = synchCache;
        this.executor = executor;
        this.numDirect = new AtomicLong(0);
        this.numDeferred = new AtomicLong(0);
        this.numRemovedNulls = new AtomicLong(0);
    }

    @Override
    public void getAsync(final K key, final CacheCallback<K, V> callback) {
         if(!isAlwaysChangeThread() && cache.containsKey(key)){
             doGetAndCallback(key, callback);
             numDirect.incrementAndGet();
         } else {
             executor.execute(new Runnable(){
                 @Override
                 public void run() {
                     doGetAndCallback(key, callback);
                     numDeferred.incrementAndGet();
                 }
             });
         }
    }

    private void doGetAndCallback(final K key, final CacheCallback<K, V> callback){
        try{
            V value= cache.get(key);
            if(value != null){
                callback.onLoad(key, value);
            } else {
                callback.onError(key, new NoDataException("no data found"));
                // do not allow nulls to stay in the cache.
                if(removeNulls) {
                    cache.invalidate(key);
                    numRemovedNulls.incrementAndGet();
                }
            }
        } catch (Throwable t){
            callback.onError(key, t);
        }
    }

    @Override
    public V getIfPresent(K key) {
        V value= null;
        if(cache.containsKey(key)){
            value = cache.get(key);
        }
        return value;
    }

    @Override
    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    @Override
    public String getName() {
        return "AsyncAdaptorFor"+ cache.getName();
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public Map<K, V> getAll(Collection<K> Key) {
        return cache.getAll(Key);
    }

    @Override
    public boolean invalidate(K key) {
        return cache.invalidate(key);
    }

    @Override
    public int invalidateAll() {
        return cache.invalidateAll();
    }

    @ManagedAttribute
    public long getNumDirect() {
        return numDirect.get();
    }

    @ManagedAttribute
    public long getNumDeferred() {
        return numDeferred.get();
    }

    @ManagedAttribute
    public boolean isRemoveNulls() {
        return removeNulls;
    }

    @ManagedAttribute
    public void setRemoveNulls(boolean removeNulls) {
        this.removeNulls = removeNulls;
    }

    @ManagedAttribute
    public AtomicLong getNumRemovedNulls() {
        return numRemovedNulls;
    }

    @ManagedAttribute
    public boolean isAlwaysChangeThread() {
        return alwaysChangeThread;
    }

    @ManagedAttribute
    public void setAlwaysChangeThread(boolean alwaysChangeThread) {
        this.alwaysChangeThread = alwaysChangeThread;
    }
}
