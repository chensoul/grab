package com.javachen.grab.common.lang;

import java.util.concurrent.locks.Lock;

/**
 * Makes a {@link java.util.concurrent.locks.Lock} into an {@link AutoCloseable} for use with try-with-resources:
 * <p/>
 * {@code
 * try (AutoLock al = new AutoLock(lock)) {
 * ...
 * }
 * }
 */
public final class AutoLock implements AutoCloseable {

    private final Lock lock;

    /**
     * Locks the given {@link java.util.concurrent.locks.Lock}.
     *
     * @param lock lock to manage
     */
    public AutoLock(Lock lock) {
        this.lock = lock;
        lock.lock();
    }

    /**
     * Unlocks the underlying {@link java.util.concurrent.locks.Lock}.
     */
    @Override
    public void close() {
        lock.unlock();
    }

}
