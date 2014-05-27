package com.betfair.caching;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class InvalidatingLRUCacheBulkTest extends LRUTestSupport {
    private AtomicLong totalTotalRequests = new AtomicLong();
    private AtomicLong totalTotalTime = new AtomicLong();

    public InvalidatingLRUCacheBulkTest() {
    }

    @Test
    public void testCacheThreaded() throws Exception {
        final int CALLS_PER_THREAD = 1000000;
        final int NUM_THREADS = 20;

        IntegerLoader loader = new IntegerLoader(false, 0);
        InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", loader, 750, 100, 9200, 7500);
        CountDownLatch latch = launchReaders(loader, cache, NUM_THREADS, CALLS_PER_THREAD);
        Random rnd = new Random(-1);
        while (latch.getCount() > 0) {
            cache.invalidate(rnd.nextInt(LRUTestSupport.NUM_KEYS));
            Thread.sleep(5);
        }
        System.out.println("Size: " + cache.getSize());
        System.out.println("Hits: " + cache.getHits());
        System.out.println("HitRate: " + (double) cache.getHits() / (CALLS_PER_THREAD * NUM_THREADS));
        System.out.println("Misses: " + cache.getMisses());
        System.out.println("Prunes: " + cache.getPruneCount());
        System.out.println("Forced Prunes: " + cache.getForcedPruneCount());
        System.out.println("InvalidationCount: " + cache.getInvalidationCount());
        System.out.println("ReadthroughMisses: " + cache.getReadthroughMisses());
        System.out.println("LastPruneDuration: " + cache.getLastPruneDuration());
        System.out.println("LastPruneRemovals: " + cache.getLastPruneRemovals());
        System.out.println("Loads: " + loader.loads);

        // Some simple assertions
        Assert.assertTrue(cache.getForcedPruneCount() == 0);
        Assert.assertTrue(cache.getPruneCount() > 0);
        Assert.assertTrue(cache.getHits() > 0);
        Assert.assertTrue(cache.getMisses() > LRUTestSupport.NUM_KEYS);
        Assert.assertEquals(cache.getMisses(), loader.loads.intValue());
    }

    @Test
    public void testCacheThreadedNoPruning() throws Exception {
        final int CALLS_PER_THREAD = 1000000;
        final int NUM_THREADS = 20;
        IntegerLoader loader = new IntegerLoader(false, 0);
        InvalidatingLRUCache<Integer, Integer> cache = new InvalidatingLRUCache<Integer, Integer>("Test", loader, 0);
        CountDownLatch latch = launchReaders(loader, cache, NUM_THREADS, CALLS_PER_THREAD);
        Random rnd = new Random(-1);
        while (latch.getCount() > 0) {
            cache.invalidate(rnd.nextInt(LRUTestSupport.NUM_KEYS));
            Thread.sleep(5);
        }
        System.out.println("Size: " + cache.getSize());
        System.out.println("Hits: " + cache.getHits());
        System.out.println("HitRate: " + (double) cache.getHits() / (CALLS_PER_THREAD * NUM_THREADS));
        System.out.println("Misses: " + cache.getMisses());
        System.out.println("Prunes: " + cache.getPruneCount());
        System.out.println("InvalidationCount: " + cache.getInvalidationCount());
        System.out.println("ReadthroughMisses: " + cache.getReadthroughMisses());
        System.out.println("LastPruneDuration: " + cache.getLastPruneDuration());
        System.out.println("LastPruneRemovals: " + cache.getLastPruneRemovals());
        System.out.println("Loads: " + loader.loads);

        // Some simple assertions
        Assert.assertTrue(cache.getPruneCount() == 0);
        Assert.assertTrue(cache.getMisses() > LRUTestSupport.NUM_KEYS);
        Assert.assertEquals(cache.getMisses(), loader.loads.intValue()); // + 100 for the 100 missing keys
        Assert.assertTrue(loader.loads.intValue() <= cache.getInvalidationCount() + LRUTestSupport.NUM_KEYS + 100); // + 100 for the 100 missing keys
    }

    @Test
    public void testCacheThreadedWithReadAhead() throws Exception {
        final int CALLS_PER_THREAD = 1000000;
        final int NUM_THREADS = 20;
        IntegerLoader loader = new IntegerLoader(false, 0);
        InvalidatingLRUCache<Integer, Integer> cache =
                new InvalidatingLRUCache<Integer, Integer>("Test", loader, 2000, Executors.newFixedThreadPool(10), 0.5);
        CountDownLatch latch = launchReaders(loader, cache, NUM_THREADS, CALLS_PER_THREAD);
        Random rnd = new Random(-1);
        while (latch.getCount() > 0) {
            int key = rnd.nextInt(LRUTestSupport.NUM_KEYS);
            Integer val = cache.get(key);
            cache.invalidate(key);
            // Immediately re-get the key and ensure it's changed
            if (cache.get(key) == val) {
                System.out.println("Value of key " + key + " is not incremented after cache invalidate");
            }
            Thread.sleep(1);
        }
        Thread.sleep(100); // let things calm down

        System.out.println("Size: " + cache.getSize());
        System.out.println("Hits: " + cache.getHits());
        System.out.println("HitRate: " + (double) cache.getHits() / (CALLS_PER_THREAD * NUM_THREADS));
        System.out.println("Misses: " + cache.getMisses());
        System.out.println("Prunes: " + cache.getPruneCount());
        System.out.println("InvalidationCount: " + cache.getInvalidationCount());
        System.out.println("ReadthroughMisses: " + cache.getReadthroughMisses());

        System.out.println("ReadAheadRequests: " + cache.getReadAheadRequests());
        System.out.println("ReadAheadMisses: " + cache.getReadAheadMisses());
        System.out.println("ReadAheadQueueSize: " + cache.getReadAheadQueueSize());

        System.out.println("LastPruneDuration: " + cache.getLastPruneDuration());
        System.out.println("LastPruneRemovals: " + cache.getLastPruneRemovals());
        System.out.println("Loads: " + loader.loads);

        // Some simple assertions
        Assert.assertTrue(cache.getPruneCount() == 0);
        Assert.assertTrue(cache.getMisses() > LRUTestSupport.NUM_KEYS);
        Assert.assertTrue(cache.getMisses() + cache.getReadAheadRequests() >= loader.loads.intValue());
        Assert.assertEquals(0, cache.getReadAheadQueueSize());

    }

    private CountDownLatch launchReaders(final IntegerLoader loader, final InvalidatingLRUCache<Integer, Integer> cache, int numReaders, final int iterations) {
        ExecutorService readers = Executors.newFixedThreadPool(numReaders);
        final CountDownLatch latch = new CountDownLatch(numReaders);
        for (int i = 0; i < numReaders; i++) {
            final int seed = i;
            readers.execute(new Runnable() {

                @Override
                public void run() {
                    Random rnd = new Random(seed);
                    long totalTimeTaken = 0;
                    for (int i = 0; i < iterations; i++) {

                        // Lets have some misses
                        int key = rnd.nextInt(LRUTestSupport.NUM_KEYS + 100);
                        totalTimeTaken -= System.nanoTime();
                        Integer value = cache.get(key);
                        totalTimeTaken += System.nanoTime();
                        if (key >= LRUTestSupport.NUM_KEYS) {
                            if (value != null) {
                                System.err.println("Got NOT null value back for " + key + ": " + value);
                            }
                        } else {
                            if (value == null) {
                                System.err.println("Got null value back for " + key + ": " + value);
                            } else {
                                int expectedVal = loader.LOAD_COUNTS[key].get();
                                if (value + 5 < expectedVal || value > expectedVal) {
                                    System.err.println("Got unexpected value back for " + key + ": " + value + ", expected " + expectedVal);
                                }
                            }
                        }
                    }
                    System.out.println("Thread " + seed + " took average time per get of " + totalTimeTaken / iterations + " ns");
                    totalTotalRequests.addAndGet(iterations);
                    totalTotalTime.addAndGet(totalTimeTaken);
                    latch.countDown();

                    if (latch.getCount() == 0) {
                        System.out.println("All threads averaged to time per get of " + totalTotalTime.longValue() / totalTotalRequests.longValue() + " ns");
                    }
                }
            });
        }
        return latch;
    }
}