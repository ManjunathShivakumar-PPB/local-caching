package com.betfair.caching;

import java.util.concurrent.ExecutorService;

/**
 * Cleaner async loader interface.
 */
public interface AsyncDataLoader<K, V>  extends Loader<K,V> {

    public void setExecutor(ExecutorService executor);

    public void loadAsync(K key, AsyncLoaderCallback<K,V> callback);
}
