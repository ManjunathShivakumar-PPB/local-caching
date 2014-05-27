package com.betfair.caching;

/*
* Allow "peek" into cache without causing a read-through load,
* as well as the standard Cache features
*/

public interface InspectableCache<K,V> extends Cache<K,V>{

    boolean containsKey(K key);

}
