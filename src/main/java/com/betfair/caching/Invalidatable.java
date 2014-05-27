package com.betfair.caching;

public interface Invalidatable<K> {
	
	/*
	 * Tells the cache/whatever to invalidate whatever is has under this key, and if it supports it, to
	 * reload it asap.
	 */
	public boolean invalidate(K key);

	public int invalidateAll();

}