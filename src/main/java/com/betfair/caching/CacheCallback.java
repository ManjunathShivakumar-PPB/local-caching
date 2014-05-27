package com.betfair.caching;

/**
 * Callback interface to an asynch cache
 */
public interface CacheCallback<K, V> {

    public void onLoad(K key, V value);

    public void onError(K key, Throwable error);

}
