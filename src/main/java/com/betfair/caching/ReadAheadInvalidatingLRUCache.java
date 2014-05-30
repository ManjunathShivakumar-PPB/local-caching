package com.betfair.caching;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@ManagedResource
public class ReadAheadInvalidatingLRUCache<K, V> implements InspectableCache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(ReadAheadInvalidatingLRUCache.class);

    private static final double HARD_CAP_RATIO = 1.1;
    private static final Object ACTIVE_KEY = new Object();
    private static final Object INVALIDATED_KEY = new Object();
    private static final Object WRITTEN_KEY = new Object();

    private static Ticker timeProvider = Ticker.systemTicker();

    private static final Timer pruneTimer = new Timer("PruneServiceTimer", true);
    private static final long MILLI_TO_NANO = 1000000L;

    private final LoadingCache<K, ValueHolder<V>> map;
    private final long ttl;
    private final Loader<K, V> loader;
    private final String name;

    private final boolean pruning;
    private AtomicLong pruneFrom;
    private AtomicLong pruneTo;
    private final int pruneInterval;

    private final ReadAheadService readAheadService;

    private final int bulkLoadCount;


    private Date lastPruneTime;
    private long lastPruneDuration;
    private long lastPruneRemovals;
    private PruneCommand pruneTask;

    private final AtomicLong pruneCount = new AtomicLong();
    private final AtomicLong forcedPruneCount = new AtomicLong();
    private final AtomicLong clearCount = new AtomicLong();
    private final AtomicLong invalidationCount = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong readthroughMisses = new AtomicLong();

    private final AtomicLong reloadOnInvalidationCount = new AtomicLong();


    /**
     * Constructs a read-through cache, no pruning, no read-ahead.
     *
     * @param name   cache name
     * @param loader a loader
     * @param ttl    time records will be stored for/expired after
     */
    public ReadAheadInvalidatingLRUCache(String name, Loader<K, V> loader, long ttl) {
        this(name, loader, ttl, 0, 0, 0, null, 0);
    }

    /**
     * Constructs a read-through cache, no pruning, but with read-ahead services.
     * <p/>
     * <p>Like in Coherence, the read-ahead time is configured as a percentage of the entry's
     * expiration time; for instance, if specified as 0.75, an entry with a one minute
     * expiration time that is accessed within fifteen seconds of its expiration will
     * be scheduled for an asynchronous reload.
     * <p/>
     * <p>ExecutorService instance must be supplied for asynchronous read-ahead threads as
     * it might be shared between different cache instances
     *
     * @param name             cache name
     * @param loader           a loader
     * @param ttl              time records will be stored for/expired after
     * @param readAheadService an executor service for asynchronous read-ahead threads
     * @param readAheadRatio   a read-ahead ratio of records' TTL
     */
    public ReadAheadInvalidatingLRUCache(String name, Loader<K, V> loader, long ttl,
                                         ExecutorService readAheadService, double readAheadRatio) {
        this(name, loader, ttl, 0, 0, 0, readAheadService, readAheadRatio);
    }

    /**
     * Constructs a read-through cache, with pruning, but no read-ahead service.
     * Prune logic is to evict least recently accessed records.
     * <p/>
     * <p>ExecutorService instance must be supplied for pruning as it might be shared
     * between different cache instances
     *
     * @param name          cache name
     * @param loader        a loader
     * @param ttl           time records will be stored for/expired after
     * @param pruneInterval prune interval
     * @param pruneFrom     prune if cache's size is bigger than this param
     * @param pruneTo       prune cache to this size
     */
    public ReadAheadInvalidatingLRUCache(String name, Loader<K, V> loader, long ttl,
                                         int pruneInterval, int pruneFrom, int pruneTo) {
        this(name, loader, ttl, pruneInterval, pruneFrom, pruneTo, null, 0);
    }

    /**
     * Constructs a read-through cache, with both pruning and read-ahead services.
     * <p/>
     * <p>Prune logic is to evict least recently accessed records.
     * <p/>
     * <p>Like in Coherence, the read-ahead time is configured as a percentage of the entry's
     * expiration time; for instance, if specified as 0.75, an entry with a one minute
     * expiration time that is accessed within fifteen seconds of its expiration will
     * be scheduled for an asynchronous reload.
     * <p/>
     * <p>ExecutorService instance must be supplied for asynchronous read-ahead threads
     * as it might be shared between different cache instances
     *
     * @param name           cache name
     * @param loader         a loader
     * @param ttl            time records will be stored for/ expired after
     * @param pruneInterval  prune interval
     * @param pruneFrom      prune if cache's size is bigger than this param
     * @param pruneTo        prune cache to this size
     * @param svc            an executor service for asynchronous read-ahead threads
     * @param readAheadRatio a read-ahead ratio of records' TTL
     */
    public ReadAheadInvalidatingLRUCache(String name, Loader<K, V> loader, long ttl,
                                         int pruneInterval, int pruneFrom, int pruneTo,
                                         ExecutorService svc, double readAheadRatio) {

        if (loader == null) throw new IllegalArgumentException("Loader is null");
        this.name = name;
        this.ttl = ttl;
        this.loader = loader;
        this.readAheadService = new ReadAheadService(svc, ttl, readAheadRatio, loader);

        CacheLoader<K, ValueHolder<V>> readthrough = new CacheLoader<K, ValueHolder<V>>() {
            @Override
            public ValueHolder<V> load(K key) throws Exception {

                misses.incrementAndGet();
                readAheadService.invalidateInflightRead(key); // not interested in read aheads for this key
                V value = ReadAheadInvalidatingLRUCache.this.loader.load(key);
                if (value == null) {
                    readthroughMisses.incrementAndGet();
                }
                return new ValueHolder<V>(value, true);
            }
        };
        CacheBuilder<Object, Object> mapMaker = CacheBuilder.newBuilder().ticker(timeProvider);
        if (ttl != 0) {
            mapMaker.expireAfterWrite(ttl, TimeUnit.MILLISECONDS);
        }
        map = mapMaker.build(readthrough);

        log.info("Creating LRU cache (" + name + ") with expiry:" + ttl +
                ", prune interval:" + pruneInterval + ", prune from:" + pruneFrom + ", prune to:" + pruneTo +
                ", read-ahead service:" + svc + ", read-ahead ratio:" + readAheadRatio);

        if (this.loader.isBulkLoadSupported()) {
            long time = System.currentTimeMillis();
            Map<K, V> initialData = this.loader.bulkLoad();
            for (Map.Entry<K, V> entry : initialData.entrySet()) {
                map.put(entry.getKey(), new ValueHolder<V>(entry.getValue(), false));
            }
            time = System.currentTimeMillis() - time;
            bulkLoadCount = initialData.size();
            log.info("Bulk loaded cache ({}) with {} entries in {} milliseconds", new Object[]{name, bulkLoadCount, time});
        } else {
            bulkLoadCount = 0;
        }
        this.pruneFrom = new AtomicLong(pruneFrom);
        this.pruneTo = new AtomicLong(pruneTo);
        this.pruneInterval = pruneInterval;

        if (pruneInterval > 0 && pruneFrom > pruneTo && pruneTo > 0) {
            this.pruneTask = new PruneCommand();
            pruneTimer.scheduleAtFixedRate(pruneTask, pruneInterval, pruneInterval);
            pruning = true;
        } else {
            pruning = false;
        }
    }

    public void shutdown() {
        if (pruneTimer != null) {
            pruneTimer.cancel();
        }
    }

    // Mostly for testing.
    public void prune() {
        if (pruneTo.get() > 0) {
            new PruneCommand().run();
        } else {
            throw new IllegalArgumentException("Invalid pruneTo value:" + pruneTo);
        }
    }

    /**
     * Retrieves a value from the cache for a key.
     * Loads a value if needed, and blocks if value is being loaded by another thread.
     * Additionally might fire a read-ahead request to reload value for the key supplied.
     *
     * @param key a key
     * @return a value
     */
    public V get(K key) {
        try {
            ValueHolder<V> holder = map.get(key);
            long nanoTime = timeProvider.read();

            if (!holder.firstLoad.getAndSet(false)) {
                hits.incrementAndGet();
                // if it wasn't a first load i.e. it wasn't a read-through
                // then we request async reload
                if (readAheadService.enabled && (nanoTime - holder.loadTime >= readAheadService.readAheadMargin)) {
                    readAheadService.scheduleReload(key);
                }

            }
            holder.lastAccess = nanoTime;

            checkSizeAndPrune();

            return holder.value;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<K, V> getAll(Collection<K> key) {

        Map<K, V> result = new HashMap<K, V>();

        for (K keyVar : key) {
            result.put(keyVar, this.get(keyVar));
        }

        return result;
    }

    @Override
    public boolean containsKey(K key) {
        return map.getIfPresent(key) != null;
    }

    @Override
    public boolean invalidate(K key) {
        invalidationCount.incrementAndGet();
        if (containsKey(key)) {
            readAheadService.invalidateInflightRead(key);
            return loadKey(key);
        }
        return false;
    }

    @Override
    @ManagedOperation
    public int invalidateAll() {
        int oldSize = getSize();
        readAheadService.invalidateAllInflightReads();

        for (K k : map.asMap().keySet()) {
            loadKey(k);
        }
        // May not be 100% accurate as the map could have mutated between the
        // size and clear calls.
        clearCount.incrementAndGet();
        return oldSize;
    }

    private boolean loadKey(K key) {
        reloadOnInvalidationCount.incrementAndGet();
        if (isReadAheadEnabled()) {
            // Do it on the read ahead service
            readAheadService.scheduleReload(key);
            return true;
        } else {
            // Do it on the thread invoking invalidation
            boolean hadKey = containsKey(key);
            V value = ReadAheadInvalidatingLRUCache.this.loader.load(key);
            map.put(key, new ValueHolder<V>(value, false));
            return hadKey;
        }
    }

    /**
     * {@code ReadAheadService} submits a task to refresh value for a key
     * taking into account last accessed time and read-ahead ratio,
     * but ignores a request if there is already a refresh task submitted
     * for that key.
     * <p/>
     * <p>Like in Coherence, the read-ahead time is configured as a percentage of the entry's
     * expiration time; for instance, if specified as 0.75, an entry with a one minute
     * expiration time that is accessed within fifteen seconds of its expiration will
     * be scheduled for an asynchronous reload.
     */
    private class ReadAheadService {

        private final long readAheadMargin;
        private final ExecutorService threadPool;
        private volatile ConcurrentMap<K, Object> readAheadRegistry = new ConcurrentHashMap<K, Object>();

        private final boolean enabled;

        private final AtomicLong readAheadRequests = new AtomicLong();
        private final AtomicLong readAheadMisses = new AtomicLong();
        private final AtomicLong inflightReadsInvalidated = new AtomicLong();
        private final AtomicLong clearAllInflightReads = new AtomicLong();


        /**
         * Creates a new instance
         *
         * @param threadPool     an ExecutorService to submit tasks to,
         * @param ttl            expiration period for entries in cache
         * @param readAheadRatio read-ahead ratio, must belong to [0,1) range
         */
        private ReadAheadService(ExecutorService threadPool, long ttl, double readAheadRatio, Loader loader) {
            if (readAheadRatio >= 1 || readAheadRatio < 0)
                throw new IllegalArgumentException("ReadAheadRatio must belong to [0,1) range");

            this.readAheadMargin = Math.round(ttl * readAheadRatio) * MILLI_TO_NANO;
            this.threadPool = threadPool;
            this.enabled = threadPool != null && ttl > 0 && readAheadRatio > 0;
        }

        /**
         * Submits a reload task for a key if:
         * <p/>
         * <ol>
         * <li>read ahead service is enabled,</li>
         * <li>there is no task submitted already for this key</li>
         * <li>and current time is within read ahead period, i.e.close enough to entry's expiration time</li>
         * </ol>
         *
         * @param key a key
         */
        public void scheduleReload(final K key) {
            if (enabled && readAheadRegistry.putIfAbsent(key, ACTIVE_KEY) == null) {
                readAheadRequests.incrementAndGet();

                threadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            applyNewKey(key, loader.load(key));
                        } finally {
                            readAheadRegistry.remove(key);
                        }
                    }
                });
            }
        }

        private void applyNewKey(K key, V value) {
            try {
                if (value == null) {
                    readAheadMisses.incrementAndGet();
                }
                if (readAheadRegistry.replace(key, ACTIVE_KEY, WRITTEN_KEY) && value != null) {
                    map.put(key, new ValueHolder<V>(value, false));
                }
            } finally {
                readAheadRegistry.remove(key);
            }

        }

        /**
         * invalidate any in flight reads if the key is being queued for readahead
         *
         * @param key a key
         */
        public void invalidateInflightRead(final K key) {
            if (enabled) {
                if (readAheadRegistry.remove(key, ACTIVE_KEY)) {
                    inflightReadsInvalidated.incrementAndGet();
                }
            }
        }

        /**
         * Remove all keys in the queue
         */
        public void invalidateAllInflightReads() {
            if (enabled) {
                clearAllInflightReads.incrementAndGet();
                // Zap everything in the registry
                readAheadRegistry.clear();
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////
    // cache pruning
    //////////////////////////////////////////////////////////////////////////////////////
    private class PruneCommand extends TimerTask {

        private AtomicBoolean running = new AtomicBoolean(false);

        @Override
        public void run() {
            if (!running.getAndSet(true)) {
                try {
                    int removals = 0;
                    long pruneFrom = ReadAheadInvalidatingLRUCache.this.pruneFrom.get();
                    long pruneTo = ReadAheadInvalidatingLRUCache.this.pruneTo.get();
                    if (map.size() > pruneFrom) {
                        lastPruneTime = new Date();
                        pruneCount.incrementAndGet();
                        long timeTaken = -timeProvider.read();
                        // We create a sorted set of the current cache
                        List<LRUInfo> lru = new ArrayList<LRUInfo>();
                        for (Map.Entry<K, ValueHolder<V>> e : map.asMap().entrySet()) {
                            lru.add(new LRUInfo(e.getValue().lastAccess, e.getKey()));
                        }
                        Collections.sort(lru);
                        // and remove until we get to the target size
                        long numToPrune = lru.size() - pruneTo;
                        for (int i = 0; i < numToPrune; i++) {
                            map.invalidate(lru.get(i).key);
                            ++removals;
                        }
                        timeTaken += timeProvider.read();

                        lastPruneRemovals = removals;
                        lastPruneDuration = timeTaken / 1000;
                    }
                } finally {
                    running.set(false);
                }
            }
        }
    }

    private class LRUInfo implements Comparable<LRUInfo> {
        final long lastAccess;
        final K key;

        public LRUInfo(long lastAccess, K key) {
            this.lastAccess = lastAccess;
            this.key = key;
        }

        public int compareTo(LRUInfo o) {
            long thisVal = this.lastAccess;
            long anotherVal = o.lastAccess;
            return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
        }
    }

    /**
     * Checks if current cache size is bigger than {@link ReadAheadInvalidatingLRUCache#HARD_CAP_RATIO} * pruneFrom and
     * does pruning in current thread.
     */
    private void checkSizeAndPrune() {
        if (pruneFrom.get() != 0 && pruneTask != null) {
            if (this.map.size() > this.pruneFrom.get() * HARD_CAP_RATIO) {
                forcedPruneCount.incrementAndGet();
                this.pruneTask.run();
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////
    // value holder
    //////////////////////////////////////////////////////////////////////////////////////
    private static final class ValueHolder<V> implements Comparable<ValueHolder<V>> {
        final long loadTime = timeProvider.read();
        volatile long lastAccess = loadTime;
        final AtomicBoolean firstLoad;
        final V value;

        public ValueHolder(V value, boolean fromReadThrough) {
            this.value = value;
            firstLoad = new AtomicBoolean(fromReadThrough);
        }

        @Override
        public int compareTo(ValueHolder<V> o) {
            return (lastAccess < o.lastAccess ? -1 : (lastAccess == o.lastAccess ? 0 : 1));
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////
    // JMX
    // ////////////////////////////////////////////////////////////////////////////////////
    @ManagedOperation
    public void resetStats() {
        this.lastPruneTime = null;
        lastPruneDuration = 0;
        lastPruneRemovals = 0;

        pruneCount.set(0);
        clearCount.set(0);
        invalidationCount.set(0);
        hits.set(0);
        misses.set(0);
        readthroughMisses.set(0);

        readAheadService.readAheadRequests.set(0);
        readAheadService.readAheadMisses.set(0);

        readAheadService.inflightReadsInvalidated.set(0);
        readAheadService.clearAllInflightReads.set(0);
        reloadOnInvalidationCount.set(0);
    }

    @ManagedAttribute
    public long getClearCount() {
        return clearCount.get();
    }

    @ManagedAttribute
    public long getInvalidationCount() {
        return invalidationCount.get();
    }

    @ManagedAttribute
    public int getSize() {
        return map.asMap().size();
    }

    @ManagedAttribute
    public long getHits() {
        return hits.get();
    }

    @ManagedAttribute
    public long getMisses() {
        return misses.get();
    }

    @ManagedAttribute
    public long getReadthroughMisses() {
        return readthroughMisses.get();
    }

    @ManagedAttribute
    public long getReadAheadRequests() {
        return readAheadService.readAheadRequests.get();
    }

    @ManagedAttribute
    public long getReadAheadMisses() {
        return readAheadService.readAheadMisses.get();
    }

    @ManagedAttribute
    public long getReadAheadQueueSize() {
        return readAheadService.readAheadRegistry.size();
    }

    @ManagedAttribute
    public long getReadAheadMargin() {
        return readAheadService.readAheadMargin / MILLI_TO_NANO;
    }

    @ManagedAttribute
    public boolean isReadAheadEnabled() {
        return readAheadService.enabled;
    }

    @ManagedAttribute
    public long getInflightReadAheadsInvalidated() {
        return readAheadService.inflightReadsInvalidated.get();
    }

    @ManagedAttribute
    public long getInflightReadAheadsCleared() {
        return readAheadService.clearAllInflightReads.get();
    }

    @ManagedAttribute
    public long getPruneCount() {
        return pruneCount.get();
    }

    @ManagedAttribute
    public long getForcedPruneCount() {
        return forcedPruneCount.get();
    }

    @ManagedAttribute
    public long getTtl() {
        return ttl;
    }

    @ManagedAttribute
    public String getName() {
        return name;
    }

    @ManagedAttribute
    public boolean isPruning() {
        return pruning;
    }

    @ManagedAttribute
    public long getPruneFrom() {
        return pruneFrom.get();
    }

    @ManagedAttribute
    public long getPruneTo() {
        return pruneTo.get();
    }

    @ManagedAttribute
    public void setPruneFrom(long newValue) {
        this.pruneFrom.set(newValue);
    }

    @ManagedAttribute
    public void setPruneTo(long newValue) {
        this.pruneTo.set(newValue);
    }

    @ManagedAttribute
    public int getPruneInterval() {
        return pruneInterval;
    }

    @ManagedAttribute
    public Date getLastPruneTime() {
        return lastPruneTime;
    }

    @ManagedAttribute(description = "last prune duration in microseconds")
    public long getLastPruneDuration() {
        return lastPruneDuration;
    }

    @ManagedAttribute
    public long getLastPruneRemovals() {
        return lastPruneRemovals;
    }

    @ManagedAttribute
    public int getBulkLoadCount() {
        return bulkLoadCount;
    }

    @ManagedAttribute
    public long getReloadOnInvalidationCount() {
        return reloadOnInvalidationCount.get();
    }

    @VisibleForTesting
    static void setTimeProvider(Ticker tp) {
        timeProvider = tp;
    }

}
