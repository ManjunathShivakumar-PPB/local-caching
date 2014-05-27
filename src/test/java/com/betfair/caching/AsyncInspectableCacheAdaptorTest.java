package com.betfair.caching;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * Test it
 * It's mostly a straight call through
 */
public class AsyncInspectableCacheAdaptorTest {

    @Mock InvalidatingLRUCache<Integer,Integer> cache;
    @Mock CacheCallback<Integer, Integer> cacheCallback;
    private Executor executor = Executors.newFixedThreadPool(2);

    private final static int TEST_KEY = 1;
    private final static int TEST_VALUE = 100;

    private final static int MAX_WAIT = 500;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetAsync() throws Exception {
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);
        final String mainThreadName = Thread.currentThread().getName();
        final CountDownLatch cacheDoneLatch = new CountDownLatch(1);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object [] args = invocationOnMock.getArguments();
                Integer key = (Integer)args[0];
                if(key == TEST_KEY) return true;
                else return false;
            }
        }).when(cache).containsKey(anyInt());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object [] args = invocationOnMock.getArguments();
                Integer key = (Integer)args[0];
                assertEquals("on main thread", mainThreadName, Thread.currentThread().getName());
                return TEST_VALUE;
            }
        }).when(cache).get(anyInt());
        adaptor.getAsync(1, cacheCallback);
        verify(cacheCallback, times(1)).onLoad(TEST_KEY, TEST_VALUE);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                assertFalse("not on main thread", mainThreadName.equals(Thread.currentThread().getName()));
                cacheDoneLatch.countDown();
                return TEST_VALUE;
            }
        }).when(cache).get(anyInt());

        adaptor.getAsync(TEST_KEY+1, cacheCallback);
        Thread.sleep(30);
        cacheDoneLatch.await(MAX_WAIT, TimeUnit.MILLISECONDS);
        verify(cacheCallback, times(2)).onLoad(anyInt(), eq(TEST_VALUE));
    }

    @Test
    public void testRemovesNulls() throws  Exception{
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);
        final CountDownLatch cacheDoneLatch = new CountDownLatch(1);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object [] args = invocationOnMock.getArguments();
                Integer key = (Integer)args[0];
                if(key == TEST_KEY) return true;
                else return false;
            }
        }).when(cache).containsKey(anyInt());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return null;
            }
        }).when(cache).get(anyInt());

        // get a null back but have removeNulls = false
        adaptor.getAsync(TEST_KEY, cacheCallback);
        verify(cacheCallback, times(1)).onError(eq(TEST_KEY), Matchers.<NoDataException>any());
        verify(cache, never()).invalidate(TEST_KEY);

        // get a null back with removeNulls = true
        adaptor.setRemoveNulls(true);
        adaptor.getAsync(TEST_KEY, cacheCallback);
        verify(cacheCallback, times(2)).onError(eq(TEST_KEY), Matchers.<NoDataException>any());
        verify(cache, times(1)).invalidate(TEST_KEY);

        // should repeat for the true async case now
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                cacheDoneLatch.countDown();
                return null;
            }
        }).when(cache).get(anyInt());
        adaptor.setRemoveNulls(false);
        adaptor.getAsync(TEST_KEY + 1, cacheCallback);
        cacheDoneLatch.await(MAX_WAIT, TimeUnit.MILLISECONDS);
        Thread.sleep(30);
        verify(cacheCallback, times(1)).onError(eq(TEST_KEY+1), Matchers.<NoDataException>any());
        verify(cache, never()).invalidate(TEST_KEY+1);

        // get a null back with removeNulls = true    Need a new latch
        final CountDownLatch newLatch = new CountDownLatch(1);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                newLatch.countDown();
                return null;
            }
        }).when(cache).get(anyInt());

        adaptor.setRemoveNulls(true);
        adaptor.getAsync(TEST_KEY + 1, cacheCallback);
        newLatch.await(MAX_WAIT, TimeUnit.MILLISECONDS);
        Thread.sleep(30);
        verify(cacheCallback, times(2)).onError(eq(TEST_KEY+1), Matchers.<NoDataException>any());
        verify(cache, times(1)).invalidate(TEST_KEY+1);
    }

    @Test
    public void testGetIfPresent() throws Exception {
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);

        // not present
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object [] args = invocationOnMock.getArguments();
                Integer key = (Integer)args[0];
                if(key == TEST_KEY) return true;
                else return false;
            }
        }).when(cache).containsKey(anyInt());
        Integer result = adaptor.getIfPresent(TEST_KEY+1);
        assertEquals("not present", null, result);

        // present
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object [] args = invocationOnMock.getArguments();
                Integer key = (Integer)args[0];
                return TEST_VALUE;
            }
        }).when(cache).get(anyInt());
        result = adaptor.getIfPresent(TEST_KEY);
        assertEquals("present", Integer.valueOf(TEST_VALUE), result);

    }

    @Test
    public void testContainsKey() throws Exception {
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);
        boolean hasIt = adaptor.containsKey(22);
        verify(cache, times(1)).containsKey(22);
    }

    @Test
    public void testGetName() throws Exception {
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return "SyncCache";
            }
        }).when(cache).getName();
        String name = adaptor.getName();
        assertEquals("name","AsyncAdaptorForSyncCache",name);
    }

    @Test
    public void testGet() throws Exception {
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);
        Integer result = adaptor.get(TEST_KEY);
        verify(cache, times(1)).get(TEST_KEY);
    }

    @Test
    public void testGetAll() throws Exception {
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);
        List<Integer> keys = new ArrayList<Integer>();
        for(int i = 0; i< 100; i++){
            keys.add(i);
        }
        Map<Integer, Integer> result = adaptor.getAll(keys);
        verify(cache, times(1)).getAll(keys);
    }

    @Test
    public void testInvalidate() throws Exception {
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);
        int randomKey = 19;
        adaptor.invalidate(randomKey);
        verify(cache, times(1)).invalidate(randomKey);
    }

    @Test
    public void testInvalidateAll() throws Exception {
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);
        adaptor.invalidateAll();
        verify(cache, times(1)).invalidateAll();
    }

    @Test
    public void testExceptionFromCache(){
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);
        final Throwable throwable = new Throwable("nah nah nah");
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return true;
            }
        }).when(cache).containsKey(anyInt());
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                throw throwable;
            }
        }).when(cache).get(anyInt());
        adaptor.getAsync(1, cacheCallback);
        verify(cacheCallback, times(1)).onError(TEST_KEY, throwable);
    }

    @Test
    public void testOrderItToChangeThreadAlways() throws InterruptedException {
        AsyncInspectableCacheAdaptor<Integer, Integer> adaptor = new AsyncInspectableCacheAdaptor<Integer, Integer>(cache, executor);
        final String mainThreadName = Thread.currentThread().getName();
        final CountDownLatch cacheDoneLatch = new CountDownLatch(1);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
              return true;
            }
        }).when(cache).containsKey(anyInt());

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                assertFalse("NOT on main thread", mainThreadName.equals(Thread.currentThread().getName()));
                cacheDoneLatch.countDown();
                return TEST_VALUE;
            }
        }).when(cache).get(anyInt());
        adaptor.setAlwaysChangeThread(true);
        adaptor.getAsync(1, cacheCallback);
        cacheDoneLatch.await(MAX_WAIT, TimeUnit.MILLISECONDS);
        Thread.sleep(30);
        assertEquals("Not timed out", 0, cacheDoneLatch.getCount());
        verify(cacheCallback, times(1)).onLoad(TEST_KEY, TEST_VALUE);
    }
}
