package com.betfair.caching;

import com.google.common.base.Ticker;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LRUTestSupport {
    protected static final int NUM_KEYS = 10000;

    protected static class ValueRememberingThread extends Thread {
        Cache<Integer, Integer> cache;
        final int key;
        volatile int value = -1;

        protected ValueRememberingThread(Cache<Integer, Integer> cache, int key) {
            this.cache = cache;
            this.key = key;
        }
        @Override
        public void run() {
            value = cache.get(key);
        }
    }

    protected static class TestTimeProvider extends Ticker {
        private AtomicLong currentNanos = new AtomicLong(0L);
        protected TestTimeProvider(long initialTime) {
            this.currentNanos.set(initialTime);
        }
        protected void reset(long initialTime) {
            this.currentNanos.set(initialTime);
        }
        public void moveTime(long time, TimeUnit timeUnit) {
            this.currentNanos.addAndGet(timeUnit.toNanos(time));
        }

        @Override
        public long read() {
            return currentNanos.get();
        }
        long getMillisTime() {
            return currentNanos.get() / 1000000L;
        }
    }

    protected static class TestTimer extends Timer {
        List<TestTimerTask> tasks = Collections.synchronizedList(new ArrayList<TestTimerTask>());
        TestTimeProvider timeProvider;
        protected TestTimer(TestTimeProvider tp) {
            this.timeProvider = tp;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        synchronized (tasks) {
                            for (Iterator<TestTimerTask> it = tasks.iterator(); it.hasNext(); ) {
                                TestTimerTask tt = it.next();
                                if (tt.callTime + tt.delay <= timeProvider.getMillisTime()) {
                                    tt.task.run();
                                    it.remove();
                                }
                            }
                        }
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }
                }
            }).start();
        }

        @Override
        public void schedule(final TimerTask task, final long delay) {
            synchronized (tasks) {
                tasks.add(new TestTimerTask(timeProvider.getMillisTime(), delay, task));
            }
        }

        @Override
        public int purge() {
            synchronized (tasks) {
                tasks.clear();
            }
            return 0;
        }

        private static class TestTimerTask {
            private TestTimerTask(long callTime, long delay, TimerTask task) {
                this.callTime = callTime;
                this.delay = delay;
                this.task = task;
            }

            public long callTime;
            public long delay;
            public TimerTask task;
        }
    }
}
