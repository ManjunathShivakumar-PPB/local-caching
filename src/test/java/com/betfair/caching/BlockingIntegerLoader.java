package com.betfair.caching;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Allow holdup of loads
 *
 * Pretty basic - there is a single latch for releasing calls to load()
 */
public class BlockingIntegerLoader implements Loader<Integer, Integer> {

    private CountDownLatch latch;
    private CountDownLatch waitingLatch;
    private AtomicInteger loadCount = new AtomicInteger();

    public BlockingIntegerLoader(CountDownLatch latch, CountDownLatch waitingLatch) {
        this.latch = latch;
        this.waitingLatch = waitingLatch;
    }

    public int getLoadCount(){
        return loadCount.get();
    }

    @Override
    public Integer load(Integer key) {
        try {
            waitingLatch.countDown();
            latch.await(5000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        loadCount.incrementAndGet();
        return key;  // or any other Integer you feel like
    }

    @Override
    public boolean isBulkLoadSupported() {
        return false;
    }

    @Override
    public Map<Integer, Integer> bulkLoad() {
        throw new UnsupportedOperationException("no bulk load on blocking loader");
    }
}
