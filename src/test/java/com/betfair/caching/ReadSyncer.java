package com.betfair.caching;

import java.util.concurrent.atomic.AtomicInteger;

public class ReadSyncer {
    AtomicInteger numWaiting = new AtomicInteger();
    volatile boolean doReturn = false;
}

