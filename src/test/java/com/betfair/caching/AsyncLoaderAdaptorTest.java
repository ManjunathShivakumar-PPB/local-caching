package com.betfair.caching;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 *   Test it
 *
 *   Since this is an asynch operation,  need to use latches to check when it is all over,
 *   before checking the results etc
 *
 *   This means having to use Mockito Answer's to get the mocks to set the latches.
 */
public class AsyncLoaderAdaptorTest {

    private AsyncLoaderAdaptor<Integer, Integer> adaptor;

    @Mock Loader<Integer, Integer> syncLoader;
    @Mock AsyncLoaderCallback<Integer, Integer> callback;
    private ExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        executorService = Executors.newFixedThreadPool(3);
        adaptor = new AsyncLoaderAdaptor<Integer, Integer>(syncLoader, executorService);
    }

    @Test
    public void testLoadAsync() throws Exception {

        final Thread mainThread = Thread.currentThread();

        // set behaviour of the loader
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object [] args = invocationOnMock.getArguments();
                Integer key = (Integer)args[0];
                assertFalse("not on main thread", Thread.currentThread().equals(mainThread));
                return Integer.valueOf(key*100);
            }
        }).when(syncLoader).load(Matchers.<Integer>any());

        final CountDownLatch completedLoadLatch = new CountDownLatch(1);

        // get the callback mock to set a flag when it is done,  so we know when to check the results
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object [] args = invocationOnMock.getArguments();
                Integer key = (Integer)args[0];
                assertFalse("not on main thread", Thread.currentThread().equals(mainThread));
                completedLoadLatch.countDown();
                return null;
            }
        }).when(callback).loadComplete(anyInt(), anyInt());

        adaptor.loadAsync(1, callback);
        // wait for the callback to happen and complete
        completedLoadLatch.await();
        verify(syncLoader, times(1)).load(Integer.valueOf(1));
        verify(callback, times(1)).loadComplete(Integer.valueOf(1), Integer.valueOf(100));

        // now many at once
        final CountDownLatch[] returnLatches = new CountDownLatch[10];

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object [] args = invocationOnMock.getArguments();
                Integer key = (Integer)args[0];
                assertFalse("not on main thread", Thread.currentThread().equals(mainThread));
                returnLatches[key].await();  // force other calls onto other threads / queue
                return Integer.valueOf(key*100);
            }
        }).when(syncLoader).load(Matchers.<Integer>any());

        final CountDownLatch allCompletedLoadLatch = new CountDownLatch(10);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object [] args = invocationOnMock.getArguments();
                Integer key = (Integer)args[0];
                assertFalse("not on main thread", Thread.currentThread().equals(mainThread));
                allCompletedLoadLatch.countDown();
                return null;
            }
        }).when(callback).loadComplete(anyInt(), anyInt());

        for(int i = 0; i< 10; i++){
            returnLatches[i] = new CountDownLatch(1);
            adaptor.loadAsync(i, callback);
        }
        for(int i = 0; i< 10; i++){
            returnLatches[i].countDown();
        }
        allCompletedLoadLatch.await();

    }

    @Test
    public void testAsyncLoadError() throws Exception{

        doThrow(new RuntimeException("Test exception")).when(syncLoader).load(anyInt());

        // get the callback mock to set a latch when it is done - so we know it is ok to verify the calls
        final CountDownLatch completedLoadLatch = new CountDownLatch(1);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object [] args = invocationOnMock.getArguments();
                Integer key = (Integer)args[0];
                completedLoadLatch.countDown();
                return null;
            }
        }).when(callback).loadComplete(anyInt(), anyInt());

        adaptor.loadAsync(1, callback);

        completedLoadLatch.await();
        verify(syncLoader, times(1)).load(Integer.valueOf(1));
        verify(callback, times(1)).loadComplete(Integer.valueOf(1), null);

    }


    @Test
    public void testLoad() throws Exception {

        when(syncLoader.load(Matchers.<Integer>any())).thenReturn(100);

        Integer result = adaptor.load(1);
        assertEquals("did call through", Integer.valueOf(100), result);

        verify(syncLoader, times(1)).load(Integer.valueOf(1));
    }

    @Test
    public void testIsBulkLoadSupported() throws Exception {

        when(syncLoader.isBulkLoadSupported()).thenReturn(false);

        boolean isSupported = adaptor.isBulkLoadSupported();
        assertEquals("did call through", false, isSupported);

        verify(syncLoader, times(1)).isBulkLoadSupported();
    }

    @Test
    public void testBulkLoad() throws Exception {

        when(syncLoader.bulkLoad()).thenReturn(new HashMap<Integer, Integer>());

        Map<Integer, Integer> fakeBulkLoad = adaptor.bulkLoad();

        verify(syncLoader, times(1)).bulkLoad();

    }
}
