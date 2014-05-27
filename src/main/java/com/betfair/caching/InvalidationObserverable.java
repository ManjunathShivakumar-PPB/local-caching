package com.betfair.caching;

/**
 * A class that implements this interface will notify observers that it has received an invalidation message
 * Observers will be notified AFTER this class has processed the invalidation.  
 */
public interface InvalidationObserverable<K> {

	public void addInvalidationObserver(Invalidatable<K> observer);
	
	public void removeInvalidationObserver(Invalidatable<K> observer);
	
}
