package com.betfair.caching;

public interface AsyncLoaderCallback<K,V> {
    public void loadComplete(K key, V value);
}
