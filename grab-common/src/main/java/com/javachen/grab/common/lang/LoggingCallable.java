package com.javachen.grab.common.lang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * A {@link java.util.concurrent.Callable} that logs errors thrown from {@link #call()}. Useful in cases where
 * it would otherwise silently disappear into an executor.
 */
public abstract class LoggingCallable<V> implements Callable<V> {

    private static final Logger log = LoggerFactory.getLogger(LoggingCallable.class);

    @Override
    public final V call() {
        try {
            return doCall();
        } catch (Exception e) {
            log.warn("Unexpected error in {}", this, e);
            throw new IllegalStateException(e);
        } catch (Throwable t) {
            log.warn("Unexpected error in {}", this, t);
            throw t;
        }
    }

    public abstract V doCall() throws Exception;

}
