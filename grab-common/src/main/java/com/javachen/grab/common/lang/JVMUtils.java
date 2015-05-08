package com.javachen.grab.common.lang;

import com.google.common.base.Preconditions;
import com.javachen.grab.common.io.IOUtils;

import java.io.Closeable;
import java.util.Deque;
import java.util.LinkedList;

/**
 * JVM-related utility methods.
 */
public final class JVMUtils {

    private static final Deque<Closeable> closeAtShutdown = new LinkedList<>();

    private JVMUtils() {
    }

    /**
     * Adds a shutdown hook that try to call {@link java.io.Closeable#close()} on the given argument
     * at JVM shutdown.
     *
     * @param closeable thing to close
     */
    public static void closeAtShutdown(Closeable closeable) {
        Preconditions.checkNotNull(closeable);
        synchronized (closeAtShutdown) {
            if (closeAtShutdown.isEmpty()) {
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (Closeable c : closeAtShutdown) {
                            IOUtils.closeQuietly(c);
                        }
                    }
                }));
            }
            closeAtShutdown.push(closeable);
        }
    }

    /**
     * @return approximate heap used, in bytes
     */
    public static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

}
