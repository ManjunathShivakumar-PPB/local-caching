package com.betfair.caching;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 *   Simple adaptor to turn a synchronous Loader *with no dynamic internal state* into an AsyncLoader
 *
 *   Returns a null value if the SyncLoader errors
 */
public class AsyncLoaderAdaptor<K,V> implements AsyncDataLoader<K,V> {

    private static final Logger log = LoggerFactory.getLogger(AsyncLoaderAdaptor.class);

    private final Loader<K,V> syncLoader;

    private ExecutorService executorService;

    public AsyncLoaderAdaptor(Loader<K,V> syncLoader, ExecutorService executorService) {
        this.syncLoader = syncLoader;
        this.executorService = executorService;
    }

    @Override
    public void setExecutor(ExecutorService executor){
        this.executorService = executor;
    }

    @Override
    public void loadAsync(K key, final AsyncLoaderCallback<K,V> callback) {
        LoadTask loadTask = new LoadTask(key, callback);
        executorService.submit(loadTask);
    }

    private class LoadTask implements Runnable{
        private final K key;
        private final AsyncLoaderCallback<K,V> callback;

        private LoadTask(K key, AsyncLoaderCallback<K,V> callback) {
            this.key = key;
            this.callback = callback;
        }

        @Override
        public void run() {
            try{
                V value = syncLoader.load(key);
                callback.loadComplete(key, value);
            } catch (Exception ex){
                // exception case
                log.warn("Exception loading : "+ex);
                callback.loadComplete(key, null);
            }
        }
    }

    // --- Support synchronous operations by simply passing through on caller thread ----

    @Override
    public V load(K key) {
        return syncLoader.load(key);
    }

    @Override
    public boolean isBulkLoadSupported() {
        return syncLoader.isBulkLoadSupported();
    }

    @Override
    public Map<K, V> bulkLoad() {
        return syncLoader.bulkLoad();
    }
}
