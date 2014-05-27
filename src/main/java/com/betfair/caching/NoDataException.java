package com.betfair.caching;

/**
 * Fairly obvious error to get,  that may want to be handled cleanly
 */
public class NoDataException extends Exception {

    public NoDataException(String message) {
        super(message);
    }

    public NoDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
