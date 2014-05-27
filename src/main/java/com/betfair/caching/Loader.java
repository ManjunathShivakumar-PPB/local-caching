package com.betfair.caching;

import java.util.Map;

public interface Loader<K, V> {
	public V load(K key);
	public boolean isBulkLoadSupported();
	public Map<K, V> bulkLoad();
	
}
