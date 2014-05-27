package com.betfair.caching;

import java.util.Collection;
import java.util.Map;

/**
 * {@code Cache} is basic
 */
public interface Cache<K,V> extends Invalidatable<K> {

    String getName();

    V get(K key);

    Map<K,V> getAll(Collection<K> Key);

}
