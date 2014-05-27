package com.betfair.caching;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class IntegerLoader implements Loader<Integer, Integer> {
        protected final AtomicInteger[] LOAD_COUNTS;
        protected final AtomicInteger loads = new AtomicInteger();
        protected final boolean syncReads;
        protected final int bulkLoadCount;
        protected final ConcurrentMap<Integer, ReadSyncer> readSync = new ConcurrentHashMap<Integer, ReadSyncer>();

        public IntegerLoader(boolean syncReads, int bulkLoadCount) {
            LOAD_COUNTS = new AtomicInteger[LRUTestSupport.NUM_KEYS];
            for (int i = 0; i < LRUTestSupport.NUM_KEYS; ++i) {
                LOAD_COUNTS[i] = new AtomicInteger();
            }
            this.syncReads = syncReads;
            this.bulkLoadCount = bulkLoadCount;
        }

        public void resetKeyValues() {
            for (int i = 0; i < LRUTestSupport.NUM_KEYS; ++i) {
                LOAD_COUNTS[i] = new AtomicInteger();
            }
            readSync.clear();
        }

        @Override
        public Integer load(Integer key) {
            loads.incrementAndGet();
            Integer result;
            if (key < 0 || key >= LRUTestSupport.NUM_KEYS) {
                result = null;
            } else {
                result = LOAD_COUNTS[key].incrementAndGet();
            }
            if (syncReads) {
                ReadSyncer sync = readSync.get(key);
                if (sync != null) {
                    sync.numWaiting.incrementAndGet();
                    while (!sync.doReturn) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            // do nothing
                        }
                    }
                    sync.numWaiting.decrementAndGet();
                }
            }
            return result;
        }

        @Override
        public Map<Integer, Integer> bulkLoad() {
            Map<Integer, Integer> bulkLoadData = new HashMap<Integer, Integer>(LRUTestSupport.NUM_KEYS);
            for (int i = 0; i < bulkLoadCount; i++) {
                bulkLoadData.put(i, LOAD_COUNTS[i].incrementAndGet());
            }
            return bulkLoadData;
        }

        @Override
        public boolean isBulkLoadSupported() {
            return bulkLoadCount > 0;
        }

    }
