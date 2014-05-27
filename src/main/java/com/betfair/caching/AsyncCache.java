package com.betfair.caching;

/**
 * Asynchronous call to a cache
 */
public interface AsyncCache<K, V> extends InspectableCache<K, V> {

    // client async call
    public void getAsync(K key, CacheCallback<K,V> callback);

    // no read-through if missing
    public V getIfPresent(K key);

}
