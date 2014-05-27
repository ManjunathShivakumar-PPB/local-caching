package com.betfair.caching;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

public class AsyncIntegerLoader extends IntegerLoader implements AsyncLoader<Integer, Integer> {
    private ExecutorService service;
    private LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<Integer>();
    private int highestDrainCount = 0; // ok for single threaded executor
    private AsyncLoaderCallback<Integer, Integer> callback;

    public AsyncIntegerLoader(boolean syncReads, int bulkLoadCount) {
        super(syncReads, bulkLoadCount);
    }


    @Override
    public void setExecutor(ExecutorService executor, final AsyncLoaderCallback<Integer, Integer> callback) {
        this.service = executor;
        this.callback = callback;
    }

    @Override
    public void loadAsync(Integer key) {
        queue.add(key);

        // Add a task to load the data
        service.submit(new Runnable() {
            @Override
            public void run() {
                List<Integer> toRead = new ArrayList(10);
                if (queue.drainTo(toRead, 10) > 0) {
                    for (Integer key: toRead) {
                        callback.loadComplete(key, load(key));
                    }
                    if (toRead.size() > highestDrainCount) {
                        highestDrainCount = toRead.size();
                    }
                }
            }
        });

    }

    public void resetKeyValues() {
        super.resetKeyValues();
        highestDrainCount = 0;
    }

    public int getHighestDrainCount() {
        return highestDrainCount;
    }
}
