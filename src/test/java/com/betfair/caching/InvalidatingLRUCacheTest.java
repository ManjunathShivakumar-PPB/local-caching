package com.betfair.caching;

import com.google.common.base.Ticker;
import com.sun.corba.se.impl.orbutil.concurrent.ReentrantMutex;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.Assert.*;


public class InvalidatingLRUCacheTest extends LRUTestSupport {
	
    private class RequestClient implements Runnable{

        private Cache<Integer, Integer> cache;
        private CountDownLatch readyLatch;
        private CountDownLatch finishedLatch;

        private RequestClient(Cache<Integer, Integer> cache, CountDownLatch readyLatch, CountDownLatch finishedLatch) {
            this.cache = cache;
            this.readyLatch = readyLatch;
            this.finishedLatch = finishedLatch;
        }

        public void run(){
            readyLatch.countDown();
            int response = cache.get(1);
            finishedLatch.countDown();
        }
    }

    @Test
    public void testParallelLoads(){
        final CountDownLatch releaseLoadLatch = new CountDownLatch(1);
        final CountDownLatch waitingLatch = new CountDownLatch(1);
        final BlockingIntegerLoader loader = new BlockingIntegerLoader(releaseLoadLatch, waitingLatch);
        final InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", loader, 0);

        // cause read-through on one thread
        CountDownLatch readyLatch1 = new CountDownLatch(1);
        CountDownLatch doneLatch1 = new CountDownLatch(1);
        Thread client1 = new Thread(new RequestClient(cache, readyLatch1, doneLatch1),"Client-1");
        client1.start();

        // request again on other thread
        CountDownLatch readyLatch2 = new CountDownLatch(1);
        CountDownLatch doneLatch2 = new CountDownLatch(1);
        Thread client2 = new Thread(new RequestClient(cache, readyLatch2, doneLatch2),"Client-2");
        client2.start();

        try {
            readyLatch1.await(500, MILLISECONDS);
            readyLatch2.await(500, MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted");
        }

        releaseLoadLatch.countDown();

        try {
            doneLatch1.await(5000, MILLISECONDS);
            doneLatch2.await(5000, MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted");
        }

        // ensure thread 2 does not also cause a load
        assertEquals("Not double loaded", 1, loader.getLoadCount());
    }

    private class PeekClient implements Runnable{

        private InspectableCache<Integer, Integer> cache;
        private CountDownLatch readyLatch;
        private CountDownLatch finishedLatch;
        private CountDownLatch doItLatch;
        private volatile Boolean hadRecord;

        private PeekClient(InspectableCache<Integer, Integer> cache, CountDownLatch readyLatch, CountDownLatch finishedLatch,
                           CountDownLatch doItLatch) {
            this.cache = cache;
            this.readyLatch = readyLatch;
            this.finishedLatch = finishedLatch;
            this.doItLatch = doItLatch;
        }

        public void run(){
            readyLatch.countDown();
            try {
                doItLatch.await(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                fail("Timeout waiting for release");
            }
            hadRecord = cache.containsKey(1);
            finishedLatch.countDown();
        }

        public Boolean getHadRecord() {
            return hadRecord;
        }
    }

    @Test
    public void testContainsKeyDuringLoad(){
        final CountDownLatch releaseLoadLatch = new CountDownLatch(1);
        final CountDownLatch waitingLatch = new CountDownLatch(1);
        final BlockingIntegerLoader loader = new BlockingIntegerLoader(releaseLoadLatch, waitingLatch);
        final InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", loader, 0);

        // cause read-through on one thread
        CountDownLatch readyLatch1 = new CountDownLatch(1);
        CountDownLatch doneLatch1 = new CountDownLatch(1);
        Thread client1 = new Thread(new RequestClient(cache, readyLatch1, doneLatch1),"Client-1");
        client1.start();

        // check cache state on other thread
        CountDownLatch readyLatch2 = new CountDownLatch(1);
        CountDownLatch doItLatch = new CountDownLatch(1);
        CountDownLatch doneLatch2 = new CountDownLatch(1);
        PeekClient peekClient = new PeekClient(cache, readyLatch2, doneLatch2, doItLatch);
        Thread client2 = new Thread(peekClient,"Client-2");
        client2.start();

        // trying to control the exact order of events here using latches...
        try {
            readyLatch1.await(500, MILLISECONDS);
            readyLatch2.await(500, MILLISECONDS);
            // make sure load is "happening"
            waitingLatch.await(5000, MILLISECONDS);
            // now make the "containsKey" call while the load is happening on thread 1
            doItLatch.countDown();
            doneLatch2.await(5000, MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted");
        }

        // now finish the load
        releaseLoadLatch.countDown();

        try {
            doneLatch1.await(5000, MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted");
        }

        // ensure thread 2 got a response of "false" while the data was loading
        assertEquals("containsKey does not lie during load", Boolean.FALSE, peekClient.getHadRecord());

    }

	@Test
	public void testInvalidate() {
		InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 0), 0);
		Assert.assertEquals(0, cache.getBulkLoadCount());
		
		Assert.assertEquals(1, cache.get(1).intValue());
		Assert.assertEquals(1, cache.get(1).intValue());
		Assert.assertEquals(1, cache.getSize());
		
		Assert.assertEquals(true, cache.invalidate(1));
		Assert.assertEquals(0, cache.getSize());
		Assert.assertEquals(false, cache.invalidate(2));
		Assert.assertEquals(0, cache.getSize());

		Assert.assertEquals(2, cache.get(1).intValue());
		Assert.assertEquals(1, cache.getSize());
		Assert.assertEquals(1, cache.get(2).intValue());
		Assert.assertEquals(1, cache.get(3).intValue());
		Assert.assertEquals(3, cache.getSize());
	}
	
    @Test
    public void testContainsKey() {
        InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 0), 0);
        Assert.assertEquals(0, cache.getBulkLoadCount());

        Assert.assertFalse(cache.containsKey(1));
        // check it didn't cause a read-through load
        Assert.assertEquals(0, cache.getSize());

        Assert.assertEquals(1, cache.get(1).intValue());
        Assert.assertEquals(1, cache.getSize());

        Assert.assertTrue(cache.containsKey(1));
        Assert.assertFalse(cache.containsKey(2));

        Assert.assertEquals(1, cache.getSize());

        Assert.assertEquals(true, cache.invalidate(1));
        Assert.assertFalse(cache.containsKey(1));
        Assert.assertFalse(cache.containsKey(2));

        Assert.assertEquals(0, cache.getSize());
    }

	@Test
	public void testJMX() throws Exception {
		InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 0), 0);
		populateCache(cache, 10, 0);
		Assert.assertEquals(0, cache.getHits());
		Assert.assertEquals(10, cache.getMisses());
		Assert.assertEquals(0, cache.getReadthroughMisses());

		cache.get(1);
		Assert.assertEquals(1, cache.getHits());
		Assert.assertEquals(10, cache.getMisses());
		Assert.assertEquals(0, cache.getReadthroughMisses());

		cache.get(-1);
		Assert.assertEquals(1, cache.getHits());
		Assert.assertEquals(11, cache.getMisses());
		Assert.assertEquals(1, cache.getReadthroughMisses());
	}

	@Test
	public void testJMXReset() throws Exception {
		InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 10), 0, 10, 20, 15);
		populateCache(cache, 21, 0);
		cache.get(-1);
		Thread.sleep(100);
		populateCache(cache, 21, 0);
		cache.invalidate(1);
		cache.invalidateAll();

		Assert.assertEquals(10, cache.getBulkLoadCount());
		Assert.assertTrue(cache.getLastPruneTime() != null);
		Assert.assertTrue(cache.getLastPruneDuration() > 0);
		Assert.assertTrue(cache.getLastPruneRemovals() > 0);
		
		Assert.assertTrue(cache.getPruneCount() > 0);
		Assert.assertTrue(cache.getClearCount() > 0);
		Assert.assertTrue(cache.getInvalidationCount() > 0);
		Assert.assertTrue(cache.getHits() > 0);
		Assert.assertTrue(cache.getMisses() > 0);
		Assert.assertTrue(cache.getReadthroughMisses() > 0);
		
		cache.resetStats();
		Assert.assertTrue(cache.getLastPruneTime() == null);
		Assert.assertTrue(cache.getLastPruneDuration() == 0);
		Assert.assertTrue(cache.getLastPruneRemovals() == 0);
		
		Assert.assertTrue(cache.getPruneCount() == 0);
		Assert.assertTrue(cache.getClearCount() == 0);
		Assert.assertTrue(cache.getInvalidationCount() == 0);
		Assert.assertTrue(cache.getHits() == 0);
		Assert.assertTrue(cache.getMisses() == 0);
		Assert.assertTrue(cache.getReadthroughMisses() == 0);
		Assert.assertEquals(10, cache.getBulkLoadCount()); // not reset
	}

    @Test
    public void testForcedPruning() throws Exception {

        // Values for pruneFrom, pruneTo and number of values loaded are set specifically so that
        // forced prune happens, but this leads to hit never happen as values get pushed out before they are requested again

        InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(true, 0), 0, 10, 20, 15);
        populateCache(cache, 25, 0);
        cache.get(-1);
        Thread.sleep(100);
        populateCache(cache, 25, 0);
        cache.invalidate(1);
        cache.invalidateAll();

        Assert.assertEquals(0, cache.getBulkLoadCount());
        Assert.assertTrue(cache.getLastPruneTime() != null);
        Assert.assertTrue(cache.getLastPruneDuration() > 0);
        Assert.assertTrue(cache.getLastPruneRemovals() > 0);

        Assert.assertTrue(cache.getForcedPruneCount() > 0);
        Assert.assertTrue(cache.getPruneCount() > 0);
        Assert.assertTrue(cache.getClearCount() > 0);
        Assert.assertTrue(cache.getInvalidationCount() > 0);
        Assert.assertTrue(cache.getHits() == 0);
        Assert.assertTrue(cache.getMisses() > 0);
        Assert.assertTrue(cache.getReadthroughMisses() > 0);
    }

	@Test
	public void testBulkLoad() throws Exception {
		IntegerLoader loader = new IntegerLoader(false, 3);
		InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", loader, 0, 0, 0, 0);
		Assert.assertEquals(3, cache.getBulkLoadCount());
		Assert.assertEquals(3, cache.getSize());
        Assert.assertEquals(0, cache.getHits());
		for (int i = 0; i < 3; ++i) {
            Assert.assertEquals(1, loader.LOAD_COUNTS[1].intValue());
			Assert.assertEquals(1, cache.get(i).intValue());
		}
        Assert.assertEquals(3, cache.getHits());
		Assert.assertFalse(cache.containsKey(3));
		Assert.assertEquals(0, loader.LOAD_COUNTS[3].intValue());
		
	}

	
	@Test
	public void testSizeReadthroughBlocking() throws Exception {
		IntegerLoader loader = new IntegerLoader(true, 0);
		loader.readSync.put(1, new ReadSyncer());
		loader.readSync.put(2, new ReadSyncer());

		final InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", loader, 0);
		ValueRememberingThread t11 = new ValueRememberingThread(cache, 1);
		t11.start();
		// Wait for thread 1 to get in the loader
		while (loader.readSync.get(1).numWaiting.get() == 0) {
			Thread.sleep(1);
		}
		
		ValueRememberingThread t21 = new ValueRememberingThread(cache, 2);
		t21.start();
		// Wait for thread 2 to get in the loader
		while (loader.readSync.get(2).numWaiting.get() == 0) {
			Thread.sleep(1);
		}

		// Start a second reader thread for the keys
		ValueRememberingThread t12 = new ValueRememberingThread(cache, 1);
		t12.start();

		ValueRememberingThread t22 = new ValueRememberingThread(cache, 1);
		t22.start();

		// Wait a little while to check the second readers don't return
		t12.join(250);
		t22.join(250);
		Assert.assertEquals(-1, t12.value); // nothing back yet
		Assert.assertEquals(-1, t22.value); // nothing back yet
		Assert.assertEquals(1, loader.readSync.get(1).numWaiting.get()); // They are not in the Loader
		Assert.assertEquals(1, loader.readSync.get(2).numWaiting.get()); // They are not in the Loader

		// release the read locks
		loader.readSync.get(1).doReturn = true;
		loader.readSync.get(2).doReturn = true;

		// Wait for it all to pan out
		t11.join();
		t12.join();
		t21.join();
		t22.join();
		
		Assert.assertEquals(1, t11.value);
		Assert.assertEquals(1, t12.value);
		Assert.assertEquals(1, t21.value);
		Assert.assertEquals(1, t22.value);
	}
	
//	@Test
	public void testInvalidateBlockingRead() throws Exception {
		IntegerLoader loader = new IntegerLoader(true, 0);
		loader.readSync.put(1, new ReadSyncer());

		final InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", loader, 0);
		ValueRememberingThread t11 = new ValueRememberingThread(cache, 1);
		t11.start();
		// Wait for thread 1 to get in the loader
		while (loader.readSync.get(1).numWaiting.get() == 0) {
			Thread.sleep(1);
		}

		// now invalidate the entry
		cache.invalidate(1);
		// fire off a read post invalidation
		ValueRememberingThread t1post = new ValueRememberingThread(cache, 1);
		t1post.start();
		// Wait for thread 1 to get in the loader
		while (loader.readSync.get(1).numWaiting.get() == 1) {
			Thread.sleep(1);
		}
		
		// release the read and see what comes back.
		loader.readSync.get(1).doReturn = true;
		t11.join();
		
		Assert.assertEquals(1, t11.value); // The immediate read comes back with the old value, this is fine
		t1post.join();
		Assert.assertEquals(2, t1post.value); // The new read is correct as the key was invalidated
		Assert.assertEquals(0, cache.getReadAheadQueueSize());
}
	@Test
	public void testSizeEvictions() throws Exception {
		long startTime = System.currentTimeMillis();
		InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 0), 0, 100, 10, 6);
		Assert.assertTrue(cache.isPruning());
		populateCache(cache, 11, 0);
		 // wait up to 10 seconds for the prune 
		boolean pruned = false;
		for (int i = 0; i < 200; ++i) {
			int size = cache.getSize();
			if (size == 6) {
				System.out.println("Prune completed after "+i*50+" ms");
				pruned = true;
				break;
			}
			Thread.sleep(50);
		}
		Assert.assertTrue(pruned);
		Assert.assertEquals(5, cache.getLastPruneRemovals());
		Assert.assertTrue(cache.getLastPruneDuration() > 0);
		Assert.assertTrue(cache.getLastPruneTime().getTime() >= startTime);
		Assert.assertTrue(cache.getLastPruneTime().getTime() <= System.currentTimeMillis());
	}

	@Test
	public void testSingleInvalidations() throws Exception {
		InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 0), 0);
		populateCache(cache, 10, 0);
		Assert.assertEquals(10, cache.getSize());
		Assert.assertEquals(0, cache.getInvalidationCount());
		Assert.assertTrue(cache.invalidate(1));
		Assert.assertEquals(1, cache.getInvalidationCount());
		Assert.assertEquals(9, cache.getSize());

		Assert.assertFalse(cache.invalidate(1));
		Assert.assertEquals(2, cache.getInvalidationCount());
		Assert.assertEquals(0, cache.getClearCount());
		Assert.assertEquals(9, cache.getSize());

		// reload
		Assert.assertEquals(2, cache.get(1).intValue());
		
	}

	@Test
	public void testClears() throws Exception {
		InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 0), 0);
		populateCache(cache, 10, 0);
		Assert.assertEquals(10, cache.getSize());
		Assert.assertEquals(0, cache.getInvalidationCount());

		Assert.assertEquals(10, cache.invalidateAll());
		Assert.assertEquals(0, cache.getSize());
		Assert.assertEquals(1, cache.getClearCount());

		// reload
		Assert.assertEquals(2, cache.get(1).intValue());
		Assert.assertEquals(1, cache.invalidateAll());
		Assert.assertEquals(0, cache.getSize());
		Assert.assertEquals(2, cache.getClearCount());
		
	}

	@Test
	public void testExpiry() throws Exception {
		InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 0), 100);
		populateCache(cache, 1, 0);
		
		Assert.assertEquals(1, cache.get(1).intValue());
		 // wait up to 10 seconds for it to leave the cache
		boolean expired = false;
		for (int i = 0; i < 200; ++i) {
			int val = cache.get(1);
			if (val == 2) {
				System.out.println("Expired after "+i*50+" ms");
				expired = true;
				break;
			}
			Thread.sleep(50);
		}
		Assert.assertTrue(expired);
	}
	
	@Test
	public void testNoExpiry() throws Exception {
		InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 0), 0);
		populateCache(cache, 1, 0);
		
		Assert.assertEquals(1, cache.get(1).intValue());
		 // wait up to 1 second for it to leave the cache. It shouldn't
		for (int i = 0; i < 20; ++i) {
			Assert.assertEquals(1, cache.get(1).intValue());
			Thread.sleep(50);
		}
	}

    @Test
    public void testGetAll() throws Exception {
        InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 0), 0);
        populateCache(cache, 3, 0);

        Collection<Integer> keys = new ArrayList<Integer> ();
        keys.add(1);
        keys.add(2);

        assertTrue(cache.getAll(keys).size() == 2);

        // invalidate the cache so it has to reload the data
        cache.invalidateAll();

        keys.add(3);

        Map<Integer, Integer> var = cache.getAll(keys);

        assertTrue(var.containsKey(1) && var.containsKey(2) && var.containsKey(3));
        assertTrue((var.get(1) == 2) && (var.get(2) == 2) && (var.get(3) == 1));
    }

	@Test
	public void testLRUEvictions() throws Exception {
		InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", new IntegerLoader(false, 0), 0);
		populateCache(cache, 3, 1);
		cache.get(0); // frig the order
		// Cache (in LRU order) = 0, 2, 1

		Thread.sleep(1); // ensure the access orders are OK
		cache.get(3); // Evict an entry (Last used was 1)
		cache.prune(3);
		Assert.assertEquals(3, cache.getSize());
		Assert.assertTrue(cache.containsKey(0)); // not evicted
		Assert.assertFalse(cache.containsKey(1)); // evicted
		Assert.assertTrue(cache.containsKey(2)); // not evicted
		Assert.assertTrue(cache.containsKey(3)); // not evicted
		
		// Cache (in LRU order) = 3, 0, 2
		Thread.sleep(1); // ensure the access orders are OK
		cache.get(4); // Evict an entry (Last used was 2)
		cache.prune(3);
		Assert.assertEquals(3, cache.getSize());
		Assert.assertTrue(cache.containsKey(0)); // not evicted
		Assert.assertFalse(cache.containsKey(1)); // evicted
		Assert.assertFalse(cache.containsKey(2)); // evicted
		Assert.assertTrue(cache.containsKey(3)); // not evicted
		Assert.assertTrue(cache.containsKey(4)); // not evicted
		
		// Cache (in LRU order) = 4, 3, 0
		Thread.sleep(1); // ensure the access orders are OK
		cache.get(4); // frig the order
		Thread.sleep(1); // ensure the access orders are OK
		cache.get(0); // frig the order
		Thread.sleep(1); // ensure the access orders are OK
		cache.get(3); // frig the order
		
		// Cache (in LRU order) = 3, 0, 4
		Thread.sleep(1); // ensure the access orders are OK
		cache.get(1); // Evict an entry
		Thread.sleep(1); // ensure the access orders are OK
		cache.get(2); // Evict another entry
		cache.prune(3);
		Assert.assertFalse(cache.containsKey(0)); // evicted
		Assert.assertTrue(cache.containsKey(1)); // not evicted
		Assert.assertTrue(cache.containsKey(2)); // not evicted
		Assert.assertTrue(cache.containsKey(3)); // not evicted
		Assert.assertFalse(cache.containsKey(4)); // evicted
	}

    @Test
    public void testCacheExpiration() throws InterruptedException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        TestTimeProvider testTimeProvider = new TestTimeProvider(System.nanoTime());

        try {
            InvalidatingLRUCache.setTimeProvider(testTimeProvider);

            InvalidatingLRUCache<Integer, Integer> cache =
                    new InvalidatingLRUCache<Integer, Integer>("Test", new Loader<Integer, Integer>() {
                        @Override
                        public Integer load(Integer key) {
                            return key;
                        }

                        @Override
                        public boolean isBulkLoadSupported() {
                            return false;
                        }

                        @Override
                        public Map<Integer, Integer> bulkLoad() {
                            return null;
                        }
                    }, 1000);


            // test before item is expired
            Integer key = 1;
            cache.get(key); // a read-through
            assertEquals(1, cache.getMisses());
            assertEquals(0, cache.getHits());

            // test that cache being hit and not the loader
            testTimeProvider.moveTime(100, MILLISECONDS); // moving time so entry is not expired yet

            cache.get(key); // this call should count as a cache hit
            assertEquals(1, cache.getMisses());
            assertEquals(1, cache.getHits());

            // wait for item to expire
            testTimeProvider.moveTime(2, TimeUnit.SECONDS);
            Thread.sleep(1000);
            cache.get(key); // a read-through
            assertEquals(2, cache.getMisses());
            assertEquals(1, cache.getHits());
        } finally {
            InvalidatingLRUCache.setTimeProvider(Ticker.systemTicker());
        }
    }

    @Test
    public void testCacheWithReadAhead() throws InterruptedException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        final ReentrantMutex reentrantMutex = new ReentrantMutex();

        TestTimeProvider testTimeProvider = new TestTimeProvider(System.nanoTime());

        try {
            InvalidatingLRUCache.setTimeProvider(testTimeProvider);

            InvalidatingLRUCache<Integer, Integer> cache =
                    new InvalidatingLRUCache<Integer, Integer>("Test", new Loader<Integer, Integer>() {
                        @Override
                        public Integer load(Integer key) {
                            try {
                                reentrantMutex.acquire();
                                reentrantMutex.release();
                            } catch (InterruptedException e) {
                                //do nothing
                            }
                            return key;
                        }

                        @Override
                        public boolean isBulkLoadSupported() {
                            return false;
                        }

                        @Override
                        public Map<Integer, Integer> bulkLoad() {
                            return null;
                        }
                    }, 1000, Executors.newSingleThreadExecutor(), 0.5);


            // test before item is expired
            Integer key = 1;
            cache.get(key); // a read-through
            assertEquals(1, cache.getMisses());
            assertEquals(0, cache.getHits());
            assertEquals(0, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());

            // test that cache being hit and not the loader
            testTimeProvider.moveTime(100, MILLISECONDS);
            cache.get(key); // this call should count as a cache hit
            assertEquals(1, cache.getMisses());
            assertEquals(1, cache.getHits());
            assertEquals(0, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());

            // test that cache being hit and request for a read-ahead is fired
            testTimeProvider.moveTime(450, MILLISECONDS);
            reentrantMutex.acquire();
            cache.get(key); // a hit
            assertEquals(1, cache.getMisses());
            assertEquals(2, cache.getHits());
            assertEquals(1, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());

            // more cache hits without a read-ahead request
            cache.get(key); // a hit but no extra read-ahead request
            cache.get(key); // a hit but no extra read-ahead request
            assertEquals(1, cache.getMisses());
            assertEquals(4, cache.getHits());
            assertEquals(1, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());
            reentrantMutex.release();

        } finally {
            InvalidatingLRUCache.setTimeProvider(Ticker.systemTicker());
        }
    }
    @Test
    public void testCacheWithReadAheadAndReadThrough() throws InterruptedException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        final ReentrantMutex reentrantMutex = new ReentrantMutex();

        TestTimeProvider testTimeProvider = new TestTimeProvider(System.nanoTime());

        try {
            InvalidatingLRUCache.setTimeProvider(testTimeProvider);

            InvalidatingLRUCache<Integer, Integer> cache =
                    new InvalidatingLRUCache<Integer, Integer>("Test", new Loader<Integer, Integer>() {
                        @Override
                        public Integer load(Integer key) {
                            try {
                                reentrantMutex.acquire();
                                reentrantMutex.release();
                            } catch (InterruptedException e) {
                                //do nothing
                            }
                            return key;
                        }

                        @Override
                        public boolean isBulkLoadSupported() {
                            return false;
                        }

                        @Override
                        public Map<Integer, Integer> bulkLoad() {
                            return null;
                        }
                    }, 1000, Executors.newSingleThreadExecutor(), 0.5);


            // test before item is expired
            Integer key = 1;
            cache.get(key); // a read-through
            assertEquals(1, cache.getMisses());
            assertEquals(0, cache.getHits());
            assertEquals(0, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());

            // test that cache being hit and not the loader
            testTimeProvider.moveTime(100, MILLISECONDS);
            cache.get(key); // this call should count as a cache hit
            assertEquals(1, cache.getMisses());
            assertEquals(1, cache.getHits());
            assertEquals(0, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());

            // test that cache being hit and request for a read-ahead is fired
            testTimeProvider.moveTime(450, MILLISECONDS);
            reentrantMutex.acquire();
            cache.get(key); // a hit
            assertEquals(1, cache.getMisses());
            assertEquals(2, cache.getHits());
            assertEquals(1, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());

            testTimeProvider.moveTime(600, MILLISECONDS);
            Thread.sleep(1000);
            cache.get(key); // a read-through even though there is a read ahead in process
            reentrantMutex.release();
            assertEquals(2, cache.getMisses());
            assertEquals(2, cache.getHits());
            assertEquals(1, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());

        } finally {
            InvalidatingLRUCache.setTimeProvider(Ticker.systemTicker());
        }
    }

    @Test
    public void testLoaderException() {
        boolean exceptionCameBack = false;
        try{
            InvalidatingLRUCache<Integer, Integer> cache =
                    new InvalidatingLRUCache<Integer, Integer>("Test", new Loader<Integer, Integer>() {
                        @Override
                        public Integer load(Integer key) {
                            throw new RuntimeException("I done bad");
                        }

                        @Override
                        public boolean isBulkLoadSupported() {
                            return false;
                        }

                        @Override
                        public Map<Integer, Integer> bulkLoad() {
                            return null;
                        }
                    }, 1000);
            Integer result = cache.get(1);
            fail("Expect an exception to be thrown out of the cache");
        } catch (Throwable t){
            // pass
            exceptionCameBack = true;
        } finally {
            assertTrue("Did get given exception", exceptionCameBack);
        }

    }

    @Test
    public void testLoaderReturnsNull() {
        try{
            InvalidatingLRUCache<Integer, Integer> cache =
                    new InvalidatingLRUCache<Integer, Integer>("Test", new Loader<Integer, Integer>() {
                        @Override
                        public Integer load(Integer key) {
                            return null;
                        }

                        @Override
                        public boolean isBulkLoadSupported() {
                            return false;
                        }

                        @Override
                        public Map<Integer, Integer> bulkLoad() {
                            return null;
                        }
                    }, 1000);
            Integer result = cache.get(1);
            assertEquals("got a null ok", null, result);
        } catch (Throwable t){
            fail("Expected a null back");
        }
    }


    private void populateCache(InvalidatingLRUCache<Integer, Integer>cache, int entries, int sleepBetweenAdditions) throws Exception {
		for (int i = 0; i < entries; ++i) {
			cache.get(i);
			if (sleepBetweenAdditions > 0) {
				Thread.sleep(sleepBetweenAdditions);
			}
		}
	}

}