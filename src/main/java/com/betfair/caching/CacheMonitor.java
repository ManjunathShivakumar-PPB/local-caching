package com.betfair.caching;

/**
 * Watch a cache.
 */
public interface CacheMonitor<K,V> {

    void onAddOrReplace(K key,  V value);

    /**
     * It is possible for a key with no value to be "removed";
     * There are two cases for this
     * 1. the Cache was in the process of loading the value when it was told to invalidate it.
     * 2. data was never known for that key (i.e. the cache was spammed an invalidate for a key that isn't relevant to it)
     *    e.g. the market was live not historic
     * Differentiate the two cases using a flag "loadWasInProgress"
     * @param key
     * @param value
     * @param loadWasInProgress
     */
    void onRemove(K key, V value, boolean loadWasInProgress);

    void onRemoveAll();

}
