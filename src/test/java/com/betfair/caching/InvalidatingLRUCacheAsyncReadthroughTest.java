package com.betfair.caching;

import com.google.common.base.Ticker;
import junit.framework.AssertionFailedError;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;


public class InvalidatingLRUCacheAsyncReadthroughTest extends LRUTestSupport {
	

    @Test
    public void testCacheWithReadAhead() throws Exception {
        TestTimeProvider testTimeProvider = new TestTimeProvider(System.nanoTime());

        try {
            InvalidatingLRUCache.setTimeProvider(testTimeProvider);
            AsyncIntegerLoader loader = new AsyncIntegerLoader(true, 0);

            InvalidatingLRUCache<Integer, Integer> cache =
                    new InvalidatingLRUCache<Integer, Integer>("Test", loader, 100000, Executors.newSingleThreadExecutor(), 0.005);


            // test before item is expired
            Integer key = 1;
            assertEquals(1, cache.get(key).intValue()); // a read-through
            assertEquals(1, cache.getMisses());
            assertEquals(0, cache.getHits());
            assertEquals(0, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());

            // test that cache being hit and not the loader
            testTimeProvider.moveTime(100, TimeUnit.MILLISECONDS);
            assertEquals(1, cache.get(key).intValue()); // cache hit
            assertEquals(1, cache.getMisses());
            assertEquals(1, cache.getHits());
            assertEquals(0, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());

            // test that cache being hit and request for a read-ahead is fired
            testTimeProvider.moveTime(450, TimeUnit.MILLISECONDS);

            // Block the read
            loader.readSync.put(key, new ReadSyncer());

            assertEquals(1, cache.get(key).intValue()); // cache hit
            assertEquals(1, cache.getMisses());
            assertEquals(2, cache.getHits());
            assertEquals(1, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());

            // more cache hits without a read-ahead request
            assertEquals(1, cache.get(key).intValue()); // cache hit
            assertEquals(1, cache.get(key).intValue()); // cache hit
            assertEquals(1, cache.getMisses());
            assertEquals(4, cache.getHits());
            assertEquals(1, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());
            assertEquals(0, loader.getHighestDrainCount());

            loader.readSync.get(1).doReturn = true;
            while (loader.getHighestDrainCount() == 0) {
                try { Thread.sleep(1); } catch (InterruptedException e) {}
            }

            assertEquals(2, cache.get(key).intValue()); // Refresh ahead finished
            assertEquals(1, cache.getMisses());
            assertEquals(5, cache.getHits());
            assertEquals(1, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());
            assertEquals(1, loader.getHighestDrainCount());

        } finally {
            InvalidatingLRUCache.setTimeProvider(Ticker.systemTicker());
        }
    }

    @Test
    public void testCacheWithReadAheadBatch() throws Exception {
        TestTimeProvider testTimeProvider = new TestTimeProvider(System.nanoTime());

        try {
            InvalidatingLRUCache.setTimeProvider(testTimeProvider);
            AsyncIntegerLoader loader = new AsyncIntegerLoader(true, 0);

            InvalidatingLRUCache<Integer, Integer> cache =
                    new InvalidatingLRUCache<Integer, Integer>("Test", loader, 100000, Executors.newSingleThreadExecutor(), 0.005);


            // test before item is expired
            Integer key1 = 1;
            Integer key2 = 2;
            Integer key3 = 3;
            assertEquals(1, cache.get(key1).intValue()); // a read-through (miss)
            assertEquals(1, cache.get(key2).intValue()); // a read-through (miss)
            assertEquals(1, cache.get(key3).intValue()); // a read-through (miss)

            // test that cache being hit and request for a read-ahead is fired
            testTimeProvider.moveTime(550, TimeUnit.MILLISECONDS);

            // Block the read
            loader.readSync.put(key1, new ReadSyncer());
            assertEquals(1, cache.get(key1).intValue()); // cache hit
            assertEquals(0, loader.getHighestDrainCount());
            assertEquals(1, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());
            assertEquals(0, loader.getHighestDrainCount());

            // Wait for thread to get in the loader
            while (loader.readSync.get(key1).numWaiting.get() == 0) {
                Thread.sleep(1);
            }

            // queue up a couple more reads
            loader.readSync.put(key2, new ReadSyncer());
            assertEquals(1, cache.get(key2).intValue()); // a hit
            assertEquals(1, cache.get(key3).intValue()); // a hit

            // release the first read
            loader.readSync.get(key1).doReturn = true;
            while (loader.getHighestDrainCount() == 0) {
                try { Thread.sleep(1); } catch (InterruptedException e) {}
            }

            assertEquals(2, cache.get(key1).intValue()); // hit
            assertEquals(3, cache.getMisses());
            assertEquals(4, cache.getHits());
            assertEquals(3, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());
            assertEquals(1, loader.getHighestDrainCount());


            loader.readSync.get(key2).doReturn = true;
            while (loader.getHighestDrainCount() == 1) {
                try { Thread.sleep(1); } catch (InterruptedException e) {}
            }

            assertEquals(2, cache.get(key1).intValue()); // Refresh ahead finished (hit)
            assertEquals(2, cache.get(key2).intValue()); // Refresh ahead finished (hit)
            assertEquals(2, cache.get(key3).intValue()); // Refresh ahead finished (hit)
            assertEquals(3, cache.getMisses());
            assertEquals(7, cache.getHits());
            assertEquals(3, cache.getReadAheadRequests());
            assertEquals(0, cache.getReadAheadMisses());
            assertEquals(2, loader.getHighestDrainCount());

        } finally {
            InvalidatingLRUCache.setTimeProvider(Ticker.systemTicker());
        }
    }

    @Test
    public void testCacheWithReadAheadAndReadThrough() throws Exception {
        TestTimeProvider testTimeProvider = new TestTimeProvider(System.nanoTime());

        try {
            InvalidatingLRUCache.setTimeProvider(testTimeProvider);
            AsyncIntegerLoader loader = new AsyncIntegerLoader(true, 0);

            InvalidatingLRUCache<Integer, Integer> cache =
                    new InvalidatingLRUCache<Integer, Integer>("Test", loader, 10000, Executors.newSingleThreadExecutor(), 0.5);

            for (int i = 0; i < 500; i++) {
                cache.invalidateAll();
                cache.resetStats();
                loader.resetKeyValues();
                testTimeProvider.reset(System.nanoTime());
                // test before item is expired
                Integer key1 = 1;
                assertEquals(1, cache.get(key1).intValue()); // a read-through (miss)

                // test that cache being hit and request for a read-ahead is fired
                testTimeProvider.moveTime(6000, TimeUnit.MILLISECONDS);

                // Block the read
                loader.readSync.put(key1, new ReadSyncer());
                assertEquals(1, cache.get(key1).intValue()); // cache hit
                assertEquals(0, loader.getHighestDrainCount());
                assertEquals(1, cache.getReadAheadRequests());
                assertEquals(0, cache.getReadAheadMisses());
                assertEquals(0, loader.getHighestDrainCount());

                // Wait for thread to get in the loader
                while (loader.readSync.get(key1).numWaiting.get() < 1) {
                    Thread.sleep(1);
                }

                // Expire the entry
                testTimeProvider.moveTime(6000, TimeUnit.MILLISECONDS);
                Thread.sleep(2); // Allow the timer to be recognised.

                ValueRememberingThread backgroundRead = new ValueRememberingThread(cache, key1);
                backgroundRead.start();

                // Wait for background thread to get in the loader
                boolean abort = false;
                while (loader.readSync.get(key1).numWaiting.get() < 2) {
                // ensure that the entry actually expired, otherwise abort this run
                    if (backgroundRead.value >= 0) {
                        System.out.println("Aborted run "+i+" - expiry did not occur");
                        // Readthrough happened
                        abort = true;
                        break;
                    }
                    Thread.sleep(1);
                }

                // Wait for both threads to complete
                loader.readSync.get(key1).doReturn = true;
                backgroundRead.join();
                while (loader.getHighestDrainCount() == 0) {
                    try { Thread.sleep(1); } catch (InterruptedException e) {}
                }

                if (!abort) {
                    try {
                        // Now we need to ensure that the final result is not the old value
                        assertEquals(3, backgroundRead.value);
                        assertEquals(3, cache.get(key1).intValue()); // cache hit

                        assertEquals(2, cache.getMisses());
                        assertEquals(2, cache.getHits());
                        assertEquals(1, cache.getReadAheadRequests());
                        assertEquals(0, cache.getReadAheadMisses());
                        assertEquals(1, loader.getHighestDrainCount());
                    } catch (AssertionFailedError e) {
                        System.out.println("Failed on run "+i);
                        throw e;
                    }
                }
                assertEquals(0, cache.getReadAheadQueueSize());
            }
        } finally {
            InvalidatingLRUCache.setTimeProvider(Ticker.systemTicker());
        }
    }
}