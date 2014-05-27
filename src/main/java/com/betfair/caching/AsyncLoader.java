package com.betfair.caching;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public interface AsyncLoader<K, V>  extends Loader<K,V> {
    public void setExecutor(ExecutorService executor, AsyncLoaderCallback<K,V> callback);
	public void loadAsync(K key);

}
